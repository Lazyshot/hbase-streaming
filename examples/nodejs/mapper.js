#!/usr/bin/env node

var stdin = process.stdin;
var stdout = process.stdout;
var input = '';

stdin.setEncoding('utf8');
stdin.on('data', function(data) {
  if (data) {
    input += data;
    while (input.match(/\r?\n/)) {
      input = RegExp.rightContext;
      proc(RegExp.leftContext);
    }
  }
});

stdin.on('end', function() {
  if (input) {
    proc(input);
  }
});

function proc(line) {
    var parts = line.split("\t");
    key = parts[0];
    val = JSON.parse(parts[1]);
    
    exec(key, val);
};

var emit = function(key, val){
	stdout.write(key + "\t" + val + "\n");
}

function exec(key, val) {
    if('pages' in val)
    {
        for(i in val.pages)
        {
            emit(i, 1);
        }
    }
}