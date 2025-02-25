package main

import (
	"database/sql"
	"errors"
	"fmt"
	"github.com/goccy/go-graphviz"
	_ "github.com/lib/pq"
	"github.com/sirupsen/logrus"
	"github.com/spf13/viper"
	"log"
)

// Определение внутренних структур БД
type (
	Parameter struct {
		ID        string `json:"id"`
		ShortName string `json:"short_name"`
		Type      string `json:"type"`
		IDPat     string
	}

	Pattern struct {
		ID        string `json:"id"`
		ShortName string `json:"short_name"`
		IDR       string
	}

	Rule struct {
		ID        string `json:"id"`
		ShortName string `json:"short_name"`
		IDKb      string
	}

	KnowledgeBase struct {
		ID        string `json:"id"`
		ShortName string `json:"short_name"`
	}
)

func createDatabaseRelationshipGraph(db *sql.DB) error {
	var (
		kbs        []KnowledgeBase
		rules      []Rule
		patterns   []Pattern
		parameters []Parameter
	)

	// Создание графа с помощью Graphviz
	g := graphviz.New()
	defer g.Close()

	graph, err := g.Graph()
	if err != nil {
		log.Fatal(err)
	}
	// Нахождение баз знаний в БД
	rowsKb, err := db.Query("SELECT * FROM knowledgebase")
	if err != nil {
		return errors.New("finding knowledgebases error")
	}
	defer rowsKb.Close()
	for rowsKb.Next() {
		kb := KnowledgeBase{}
		if err = rowsKb.Scan(&kb.ID, &kb.ShortName); err != nil {
			return errors.New("reading knowledgebases error")
		}
		kbs = append(kbs, kb)
		// Нахождение правил базы знаний в БД
		idkb := kb.ID
		queryRule := "SELECT * FROM rule WHERE idkb = $1"
		rowsrule, errkb := db.Query(queryRule, idkb)
		if errkb != nil {
			return errors.New("finding rules error")
		}
		defer rowsrule.Close()
		for rowsrule.Next() {
			rule := Rule{}
			if err = rowsrule.Scan(&rule.ID, &rule.ShortName, &rule.IDKb); err != nil {
				return errors.New("reading rules error")
			}
			rules = append(rules, rule)
			// Нахождение паттерна правила в БД
			idr := rule.ID
			queryPat := "SELECT * FROM pattern WHERE idr = $1"
			rowspat, errR := db.Query(queryPat, idr)
			if errR != nil {
				return errors.New("finding patterns error")
			}
			defer rowspat.Close()
			for rowspat.Next() {
				pattern := Pattern{}
				if err = rowspat.Scan(&pattern.ID, &pattern.ShortName, &pattern.IDR); err != nil {
					return errors.New("reading patterns error")
				}
				patterns = append(patterns, pattern)
				// Нахождение параметров паттерна в БД
				idpat := pattern.ID
				queryPar := "SELECT * FROM parameter WHERE idpat = $1"
				rowspar, errP := db.Query(queryPar, idpat)
				if errP != nil {
					return errors.New("finding parameters error")
				}
				defer rowspar.Close()
				for rowspar.Next() {
					parameter := Parameter{}
					if err = rowspar.Scan(&parameter.ID, &parameter.ShortName, &parameter.Type, &parameter.IDPat); err != nil {
						return errors.New("reading parameters error")
					}
					parameters = append(parameters, parameter)

				}
			}
		}
	}
	// Создание узлов для каждой записи в knowledgeBases
	for _, kb := range kbs {
		nodeKb, _ := graph.CreateNode(kb.ShortName)
		nodeKb.SetLabel(fmt.Sprintf("KNOWLEDGEBASE\n ID: %s\n Name: %s", kb.ID, kb.ShortName))
		for _, r := range rules {
			nodeRule, _ := graph.CreateNode(r.ShortName)
			if r.IDKb == kb.ID {
				nodeRule.SetLabel(fmt.Sprintf("RULE\n ID: %s\n Name: %s", r.ID, r.ShortName))
				graph.CreateEdge("Knowledgebase-Rule", nodeKb, nodeRule)
			}
			for _, pat := range patterns {
				nodePat, _ := graph.CreateNode(pat.ShortName)
				if pat.IDR == r.ID {
					nodePat.SetLabel(fmt.Sprintf("PATTERN\n ID: %s\n Name: %s", pat.ID, pat.ShortName))
					graph.CreateEdge("Rule-Pattern", nodeRule, nodePat)
				}
				for _, par := range parameters {
					nodePar, _ := graph.CreateNode(par.ID)
					nodePar.SetLabel(fmt.Sprintf("Parameter\n ID: %s\n Name: %s\n Type: %s", par.ID, par.ShortName, par.Type))
					if par.IDPat == pat.ID {
						graph.CreateEdge("Pattern-Parameter", nodePat, nodePar)
					}
				}
			}
		}
	}
	// Создание изображения графа
	if err = g.RenderFilename(graph, graphviz.PNG, "pictures/graph.png"); err != nil {
		return errors.New("rendering graph error")
	}
	return nil
}

func DBConnection() *sql.DB {
	connStr := "user=" + viper.GetString("user") + " password=" + viper.GetString("password") +
		" dbname=" + viper.GetString("dbname") + " sslmode=" + viper.GetString("sslmode")
	db, err := sql.Open("postgres", connStr)
	if err != nil {
		logrus.Fatalf("error data base connection: %s", err.Error())
	}
	return db
}

func initConfig() error {
	viper.AddConfigPath("configs")
	viper.SetConfigName("config")
	return viper.ReadInConfig()
}

func main() {
	if err := initConfig(); err != nil {
		logrus.Fatalf("error initializing configs: %s", err.Error())
	}
	db := DBConnection()
	defer db.Close()
	if err := createDatabaseRelationshipGraph(db); err != nil {
		logrus.Fatalf("error rendering graph: %s", err.Error())
	}
}
