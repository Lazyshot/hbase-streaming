package main

import (
    "os"
    "bufio"
    "encoding/json"
    "strings"
    "fmt"
    "strconv"
)


func proc(key string, values []interface{}) {
    tot := 0

    for _, num := range values {
        val, _ := strconv.Atoi(num.(string))
        tot += val
    }

    fmt.Printf("%s\t%d\n", key, tot)
}

func main() {
    r := bufio.NewReader(os.Stdin)

    for {
        var data interface{}

        line, _, _ := r.ReadLine()

        lines := strings.Split(string(line), "\t")

        if len(lines) < 2 {
            continue
        }

        key := lines[0]
        json.Unmarshal([]byte(lines[1]), &data)
        m := data.([]interface{})

        proc(key, m)

        fmt.Printf("|next|\n")
    }
}

