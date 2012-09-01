#!/usr/bin/php

<?php

function proc($key, $val){

        incCounter("Users", "Total");

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

while(true)
{
		$line = trim(fgets(STDIN));
		$parts = explode("\t", $line);

        if(count($parts) < 2)
                fwrite(STDERR, "issue with input: " . $line);
        else
                proc($parts[0], json_decode($parts[1], true));

        echo "|next|\n";
}



