<?php
include_once '../gearman_includes.php';
include_once '../../includes.php';

$worker = new GearmanWorker();

$worker->addServer(Gearman_Monitor::$host, Gearman_Monitor::$port);

global $g_db;
$g_db = new Gearman_Db();
$g_db->log_insert('Clocker Start');

//стартуем задачу сразу при запуске воркера
global $gclient;
$gclient = new GearmanClient();
$gclient->addServer();
$gclient->doBackground('clock', '');


$worker->addFunction('clock', 'clock');
function clock(GearmanJob $job){
    global $gclient;
    global $g_db;
    while(true){

        $hours = intval(date('G', time()));
        $minutes = intval(date('i', time()));

        $g_db->log_insert(date('G:i', time()));

        if(($hours >= 2 && $hours < 6) && ($minutes >= 27 && $minutes < 29)){
            $g_db->log_insert(date('G:i', time()));
            Ajax::items_get(7);
            $gclient->doBackground('clock', '');
            return;
        }
        sleep(30);
    }
}

while($worker->work()){}
 