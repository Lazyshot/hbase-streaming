#!/usr/bin/php

<?php

function proc($key, $val){

        incCounter("Users", "Total");
        incCounter("Users", "NumFriends", count($val['friends']));
        
        if(empty($val['pages']))
                return;

        incCounter("Users", "NumPages", count($val['pages']));


        foreach($val['pages'] as $page_id => $val){
                emit($page_id, 1);
        }

}

function incCounter($group, $counter, $inc = 1)
{
        fwrite(STDERR, "reporter:counter:" . $group . "," . $counter . "," . $inc . "\n");
}

function emit($key, $val)
{
        echo $key . "\t" . $val . "\n";
}

while($line = trim(fgets(STDIN)))
{

        $parts = explode("\t", $line);
        proc($parts[0], json_decode($parts[1], true));
}



