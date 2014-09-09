<?php
include_once '../gearman_includes.php';
include_once '../../includes.php';

$worker = new GearmanWorker();

$worker->addServer(Gearman_Monitor::$host, Gearman_Monitor::$port);

$g_db = new Gearman_Db();
$g_db->log_insert('Worker Start');

$worker->addFunction('page_with_offer_get', 'page_with_offer_get');
function page_with_offer_get(GearmanJob $job){

    $db = new Hp_Db();
    $g_db = new Gearman_Db();

    $data = unserialize($job->workload());
    $url = $data['url'];
    $link = trim($data['link']);
    $cookie = $data['cookie'];//это массив
    $vendor_id = $data['vendor_id'];//вендор. нужен только для вставки в БД
    $region_id = $data['region_id'];
    $session_timestamp = $data['session_timestamp'];

    try{
        $scanner = new Scanner($url, array(), array($cookie));
    }
    //невозможно получить контент целевого $url
    catch(Exception $e){

        $g_db->log_insert($e->getMessage() . ' url ' . $url);

        $gclient = new GearmanClient();
        $gclient->addServer();
        $gclient->doBackground('page_with_offer_get', serialize($data));
        $g_db->log_insert('Phoenix for Offers');

        return;
    }

    try{
        $offers = $scanner->offers_shops_get2();
    }
    //полученный с целевого $url контент - не тот HTML, который нужен для парсинга, или вообще не HTML
    catch(Exception $e){

        $g_db->log_insert($e->getMessage() . ' url ' . $url);

        $gclient = new GearmanClient();
        $gclient->addServer();
        $gclient->doBackground('page_with_offer_get', serialize($data));
        $g_db->log_insert('Phoenix for Offers');

        return;
    }

    if(count($offers) > 0){

        foreach($offers as $o){

            if(!$db->model_get_by_url($link)){
                $model = $scanner->model_data();
                $g_db->log_insert($model['model_name'] . ' URL: ' . $model['hotline_url']);
                $db->model_insert($model['model_name'], trim($model['hotline_url']));
            }

            if(!$db->shop_select_by_link($o['shop_link'])){
                $g_db->log_insert($o['shop_link']);
                $shop_scanner = new Scanner('http://hotline.ua' . $o['shop_link']);
                $shop = $shop_scanner->shop_data();
                $db->shop_insert($o['shop_link'], $o['shop_rate'], $shop['url'], $shop['name'], $shop['city'], $shop['phones']);
                unset($shop_scanner);
            }

            $db->offer_insert($vendor_id, $region_id, $link, $o['shop_name'], $o['shop_link'], $o['shop_rate'],
                                $o['price'], $o['price_link'], $o['offer_model_descr'], $session_timestamp);

        }

        $g_db->log_insert('url ' . $url . ' OK');

    }
    else{
        $g_db->log_insert('url ' . $url . ' - No Offers');
    }

    unset($scanner);
}

while($worker->work()){}

 
