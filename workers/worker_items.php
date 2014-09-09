<?php
include_once '../gearman_includes.php';
include_once '../../includes.php';

$worker = new GearmanWorker();

global $gclient;
$gclient = new GearmanClient();
$gclient->addServer();

$worker->addServer(Gearman_Monitor::$host, Gearman_Monitor::$port);

$g_db = new Gearman_Db();
$g_db->log_insert('Worker Start');

$worker->addFunction('page_with_items_get', 'page_with_items_get');
function page_with_items_get(GearmanJob $job){

    global $gclient;

    $db = new Hp_Db();
    $g_db = new Gearman_Db();


    $data = unserialize($job->workload());
    $url = $data['url'];//чистый URL, без GET параметров
    $cookie = $data['cookie'];//это массив
    $page_index = $data['page_index'];//номер страницы, начиная с 0. меньше отображаемого номера на 1
    $vendor_id = $data['vendor_id'];//вендор. нужен только для вставки в БД
    $region_id = $data['region_id'];
    $session_timestamp = $data['session_timestamp'];

    try{
        $scanner = new Scanner($url, array('p' => $page_index), array($cookie));
    }
    //невозможно получить контент целевого $url
    catch(Exception $e){
        $g_db->log_insert('url ' . $url . ' FAIL! msg: ' . $e->getMessage());

        $gclient = new GearmanClient();
        $gclient->addServer();
        $gclient->doBackground('page_with_items_get', serialize($data));
        $g_db->log_insert('Phoenix for Items');

        return;
    }

    try{
        $items = $scanner->items_on_page_get();
    }
    //полученный с целевого $url контент - не тот HTML, который нужен для парсинга, или вообще не HTML
    catch(Exception $e){

        $g_db->log_insert($e->getMessage() . ' url ' . $url);

        $gclient = new GearmanClient();
        $gclient->addServer();
        $gclient->doBackground('page_with_items_get', serialize($data));
        $g_db->log_insert('Phoenix for Items');

        return;
    }

    foreach($items as $i){
        $db->item_insert($vendor_id, $region_id, $i['name'], $i['link'], $i['price_avg'],
                            $i['price_min'], $i['price_max'], $i['count'], $session_timestamp);

        //добавляем задачу на сервер очередей
        $data_for_gearman = array(
            'url' => 'http://hotline.ua' . $i['link'] . '?tab=2',
            'link' => $i['link'],
            'cookie' => $cookie,
            'vendor_id' => $vendor_id,
            'region_id' => $region_id,
            'session_timestamp' => $session_timestamp,
        );

        $gclient->doBackground('page_with_offer_get', serialize($data_for_gearman));


    }

    $g_db->log_insert('url ' . $url . ' OK');

    $db->semaphore_set(-1);//декремент семафора, СВОБОДНО!
}

while($worker->work()){}

 
