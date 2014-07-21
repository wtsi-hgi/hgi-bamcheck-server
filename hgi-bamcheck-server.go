// Copyright (c) 2014 Genome Research Ltd.
// Author: Joshua C. Randall <jcrandall@alum.mit.edu>
//
// This program is free software: you can redistribute it and/or modify it under
// the terms of the GNU General Public License as published by the Free Software
// Foundation; either version 3 of the License, or (at your option) any later
// version.
//
// This program is distributed in the hope that it will be useful, but WITHOUT
// ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
// FOR A PARTICULAR PURPOSE. See the GNU General Public License for more
// details.
//
// You should have received a copy of the GNU General Public License along with
// this program. If not, see <http://www.gnu.org/licenses/>.
//

// hgi-bamcheck-server
// exposes lanelet bamcheck files via a web service (lookup via vrpipe database)
package main

import (
	"database/sql"
	"flag"
	"fmt"
	goproperties "github.com/dmotylev/goproperties"
	"github.com/gorilla/mux"
	_ "github.com/ziutek/mymysql/godrv"
	"io"
	"log"
	"net/http"
	"os"
)

var configFile string

func init() {
	flag.StringVar(&configFile, "config_file", "hgi-bamcheck-server.conf", "Configuration file")
}

var db *sql.DB

func main() {
	log.Print("[hgi-bamcheck-server] starting.")
	flag.Parse()

	log.Printf("[hgi-bamcheck-server] loading config from %s...", configFile)
	config, err := goproperties.Load(configFile)
	if err != nil {
		log.Printf("[hgi-bamcheck-server] could not load config file %s: %s", configFile, err)
	} else {
		log.Printf("[hgi-bamcheck-server] loaded config.")
	}

	dbaddr := fmt.Sprintf("%s:%s:%s*%s/%s/%s", config["db.scheme"], config["db.host"], config["db.port"], config["db.name"], config["db.user"], config["db.pass"])
	log.Printf("[hgi-bamcheck-server] connecting to mysql %s...", dbaddr)
	db, err = sql.Open("mymysql", dbaddr)
	if err != nil {
		log.Printf("[hgi-bamcheck-server] error opening SQL connection to " + dbaddr + ": " + err.Error())
		os.Exit(1)
	} else {
		log.Printf("[hgi-bamcheck-server] connected.")
	}
	defer db.Close()

	r := mux.NewRouter()
	r.HandleFunc("/lanelet/{lanelet}", LaneletHandler).Methods("GET").Name("lanelet")
	r.HandleFunc("/", LaneletHandler).Methods("GET").Name("root")

	http.Handle("/", r)
	bindaddr := config["bindaddr"]
	log.Printf("[hgi-bamcheck-server] starting http listener on %s", bindaddr)
	log.Fatal(http.ListenAndServe(bindaddr, nil))
}

func LaneletHandler(w http.ResponseWriter, req *http.Request) {
	params := mux.Vars(req)
	var lanelet string = params["lanelet"]
	if lanelet == "" {
		lanelet = req.FormValue("lanelet")
	}
	var laneletfile string
	log.Printf("[LaneletHandler] executing database query for lanelet %s...\n", lanelet)
	err := db.QueryRow("select f.path as path from file as f join keyvallistmember as lane on f.keyvallist = lane.keyvallist AND lane.keyval_key = 'lane' AND lane.val = ? join stepoutputfile as sof on sof.file = f.id AND sof.output_key = 'bamcheck_files' ORDER BY f.mtime DESC LIMIT 1;", lanelet).Scan(&laneletfile)
	switch {
	case err == sql.ErrNoRows:
		log.Printf("[LaneletHandler] query returned no rows for lanelet %s", lanelet)
		w.WriteHeader(404)
		io.WriteString(w, fmt.Sprintf("Lanelet %s not found", lanelet))
	case err != nil:
		log.Printf("[LaneletHandler] queryRow returned error %s", err)
		w.WriteHeader(500)
		io.WriteString(w, "Error: "+err.Error())
	default:
		log.Printf("[LaneletHandler] have laneletfile %s for lanelet %s", laneletfile, lanelet)
		fi, err := os.Stat(laneletfile)
		if err != nil {
			w.WriteHeader(500)
			io.WriteString(w, "Error: "+err.Error())
		} else {
			laneletreader, err := os.Open(laneletfile)
			if err == nil {
				w.Header().Set("Content-Length", fmt.Sprintf("%d", fi.Size()))
				w.WriteHeader(200)
				n, err := io.Copy(w, laneletreader)
				if err == nil {
					log.Printf("[LaneletHandler] wrote %d bytes of %s", n, laneletfile)
				} else {
					log.Printf("[LaneletHandler] error writing %s, %d bytes written: %v", laneletfile, n, err)
				}
			} else {
				log.Printf("[LaneletHandler] error opening file %s: %v", laneletfile, err)
				w.WriteHeader(500)
			}
		}
	}
	return
}
