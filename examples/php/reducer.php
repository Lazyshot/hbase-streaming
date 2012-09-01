#!/usr/bin/php

<?php

function proc($key, $vals){
        $sum = 0;

        foreach($vals as $val){
                $sum += intval($val);
        }

        emit($page_id, $sum);
}

function emit($key, $val)
{
        echo $key . "\t" . $val . "\n";
}

while(true)
{
		$line = trim(fgets(STDIN));
        $parts = explode("\t", $line);
        proc($parts[0], explode(",", $parts[1]));
        echo "|next|\n";
}



