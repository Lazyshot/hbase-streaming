package main

import (
    "os"
    "bufio"
    "encoding/json"
    "strings"
    "fmt"
)


func proc(key string, values map[string]interface{}) {
    pages := values["pages"].(map[string]interface{})

    for page, _ := range pages {
        fmt.Printf("%s\t%d\n", page, len(pages))
    }
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
        m := data.(map[string]interface{})

        proc(key, m)

        fmt.Printf("|next|\n")
    }
}
