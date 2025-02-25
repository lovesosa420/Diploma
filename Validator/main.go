package main

import (
	"database/sql"
	"errors"
	"github.com/gofiber/fiber/v2"
	"github.com/gofiber/fiber/v2/middleware/cors"
	_ "github.com/lib/pq"
	"github.com/sirupsen/logrus"
	"github.com/spf13/viper"
)

// Определение БД
type MyDB struct {
	*sql.DB
}

// Определение внутренних структур БД
type (
	Parameter struct {
		ID        string `json:"id"`
		ShortName string `json:"short_name"`
		Type      string `json:"type"`
		IDPattern string
	}

	Pattern struct {
		ID        string `json:"id"`
		ShortName string `json:"short_name"`
		IDRule    string
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

// Определений запросов на изменение структур БД
type (
	UpdateKbRequest struct {
		ID           string `json:"id"`
		NewShortname string `json:"new_shortname" validate:"required,min=1,max=100"`
	}

	UpdateRuleRequest struct {
		ID           string `json:"id"`
		NewShortname string `json:"new_shortname" validate:"required,min=1,max=100"`
	}

	UpdatePatternRequest struct {
		ID           string `json:"id"`
		NewShortname string `json:"new_shortname" validate:"required,min=1,max=100"`
	}

	UpdateParameterRequest struct {
		ID           string `json:"id"`
		NewShortname string `json:"new_shortname" validate:"required,min=1,max=100"`
		NewType      string `json:"new_type" validate:"required,min=1,max=100"`
	}
)

// Определение обработчика запросов
type Handler struct {
	Storage *MyDB
}

func (db *MyDB) GetAllKbs() ([]KnowledgeBase, error) {
	response, err := db.Query("select * from knowledgebase")
	if err != nil {
		return nil, errors.New("finding knowledgebases error")
	}
	defer response.Close()
	var kblist []KnowledgeBase
	for response.Next() {
		kb := KnowledgeBase{}
		err = response.Scan(&kb.ID, &kb.ShortName)
		if err != nil {
			return nil, errors.New("reading knowledgebases error")
		}
		kblist = append(kblist, kb)
	}
	return kblist, nil
}

func (db *MyDB) GetKb(id string) (KnowledgeBase, error) {
	query := "SELECT * FROM knowledgebase WHERE idkb = $1"
	row := db.QueryRow(query, id)
	kb := KnowledgeBase{}
	err := row.Scan(&kb.ID, &kb.ShortName)
	if err != nil {
		return KnowledgeBase{}, errors.New("error reading knowledgebase")
	}

	return kb, nil
}

func (db *MyDB) GetAllRules(idkb string) ([]Rule, error) {
	query := "SELECT idr, shortname, idkb FROM rule WHERE idkb = $1"
	rows, err := db.Query(query, idkb)
	if err != nil {
		return nil, errors.New("finding rules error")
	}
	defer rows.Close()
	var rulelist []Rule
	for rows.Next() {
		rule := Rule{}
		err := rows.Scan(&rule.ID, &rule.ShortName, &rule.IDKb)
		if err != nil {
			return nil, errors.New("reading rules error")
		}
		rulelist = append(rulelist, rule)
	}
	return rulelist, nil
}

func (db *MyDB) GetRule(id string) (Rule, error) {
	query := "SELECT idr, shortname, idkb FROM rule WHERE idr = $1"
	row := db.QueryRow(query, id)
	rule := Rule{}
	err := row.Scan(&rule.ID, &rule.ShortName, &rule.IDKb)
	if err != nil {
		return Rule{}, errors.New("error reading rule")
	}
	return rule, nil
}

func (db *MyDB) GetPattern(id string) (Pattern, error) {
	query := "SELECT idpat, shortname, idr FROM pattern WHERE idr = $1"
	row := db.QueryRow(query, id)
	pattern := Pattern{}
	err := row.Scan(&pattern.ID, &pattern.ShortName, &pattern.IDRule)
	if err != nil {
		return Pattern{}, errors.New("error reading pattern")
	}
	return pattern, nil
}

func (db *MyDB) GetAllParameters(idpat string) ([]Parameter, error) {
	query := "SELECT idpar, shortname, type, idpat FROM parameter WHERE idpat = $1"
	rows, err := db.Query(query, idpat)
	if err != nil {
		return nil, errors.New("finding parameters error")
	}
	defer rows.Close()
	var parameterlist []Parameter
	for rows.Next() {
		parameter := Parameter{}
		parErr := rows.Scan(&parameter.ID, &parameter.ShortName, &parameter.Type, &parameter.IDPattern)
		if parErr != nil {
			return nil, errors.New("reading parameters error")
		}
		parameterlist = append(parameterlist, parameter)
	}
	if len(parameterlist) == 0 {
		return nil, errors.New("finding parameters error")
	}
	return parameterlist, nil
}

func (db *MyDB) GetParameter(id string) (Parameter, error) {
	query := "SELECT idpar, shortname, type, idpat FROM parameter WHERE idpar = $1"
	row := db.QueryRow(query, id)
	parameter := Parameter{}
	err := row.Scan(&parameter.ID, &parameter.ShortName, &parameter.Type, &parameter.IDPattern)
	if err != nil {
		return Parameter{}, errors.New("error reading parameter")
	}
	return parameter, nil
}

func (db *MyDB) UpdateKb(request UpdateKbRequest) error {
	query := "SELECT shortname FROM knowledgebase WHERE idkb <> $1"
	response, err := db.Query(query, request.ID)
	if err != nil {
		return errors.New("finding knowledgebases error")
	}
	defer response.Close()
	for response.Next() {
		kb := KnowledgeBase{}
		err = response.Scan(&kb.ShortName)
		if err != nil {
			return errors.New("reading knowledgebases error")
		}
		if request.NewShortname == kb.ShortName {
			return errors.New("such knowledgebase shortname is already exists")
		}
		if request.NewShortname == "" {
			return errors.New("empty shortname is not allowed")
		}
	}
	queryUpdate := "UPDATE knowledgebase SET shortname = $1 WHERE idkb = $2"
	_, err = db.Exec(queryUpdate, request.NewShortname, request.ID)
	if err != nil {
		return errors.New("changing knowledgebase shortname error")
	}
	return nil
}

func (db *MyDB) UpdateRule(request UpdateRuleRequest) error {
	query := "SELECT shortname FROM rule WHERE idr <> $1"
	response, err := db.Query(query, request.ID)
	if err != nil {
		return errors.New("finding rule error")
	}
	defer response.Close()
	for response.Next() {
		rule := Rule{}
		err = response.Scan(&rule.ShortName)
		if err != nil {
			return errors.New("reading rule error")
		}
		if request.NewShortname == rule.ShortName {
			return errors.New("such rule shortname is already exists")
		}
		if request.NewShortname == "" {
			return errors.New("empty shortname is not allowed")
		}
	}
	queryUpdate := "UPDATE rule SET shortname = $1 WHERE idr = $2"
	_, err = db.Exec(queryUpdate, request.NewShortname, request.ID)
	if err != nil {
		return errors.New("changing rule shortname error")
	}
	return nil
}

func (db *MyDB) UpdatePattern(request UpdatePatternRequest) error {
	query := "SELECT shortname FROM pattern WHERE idpat <> $1"
	response, err := db.Query(query, request.ID)
	if err != nil {
		return errors.New("finding pattern error")
	}
	defer response.Close()
	for response.Next() {
		pattern := Pattern{}
		err = response.Scan(&pattern.ShortName)
		if err != nil {
			return errors.New("reading pattern error")
		}
		if request.NewShortname == pattern.ShortName {
			return errors.New("such pattern shortname is already exists")
		}
		if request.NewShortname == "" {
			return errors.New("empty shortname is not allowed")
		}
	}
	queryUpdate := "UPDATE pattern SET shortname = $1 WHERE idpat = $2"
	_, err = db.Exec(queryUpdate, request.NewShortname, request.ID)
	if err != nil {
		return errors.New("changing pattern shortname error")
	}
	return nil
}

func (db *MyDB) UpdateParameters(request []UpdateParameterRequest) error {
	for _, elem := range request {
		queryUpdate := "UPDATE parameter SET shortname = $1, type = $2 WHERE idpar = $3"
		_, err := db.Exec(queryUpdate, elem.NewShortname, elem.NewType, elem.ID)
		if err != nil {
			return errors.New("changing parameter shortname error")
		}
	}
	return nil
}

func (handler *Handler) GetAllKbs(c *fiber.Ctx) error {
	response, err := handler.Storage.GetAllKbs()
	if err != nil {
		return c.SendStatus(fiber.StatusNoContent)
	}
	return c.Status(fiber.StatusOK).JSON(response)
}

func (handler *Handler) GetKb(c *fiber.Ctx) error {
	response, err := handler.Storage.GetKb(c.Params("id"))
	if err != nil {
		return c.SendStatus(fiber.StatusNoContent)
	}
	return c.Status(fiber.StatusOK).JSON(response)
}

func (handler *Handler) GetAllRules(c *fiber.Ctx) error {
	response, err := handler.Storage.GetAllRules(c.Params("idkb"))
	if err != nil {
		return c.SendStatus(fiber.StatusNoContent)
	}
	return c.Status(fiber.StatusOK).JSON(response)
}

func (handler *Handler) GetRule(c *fiber.Ctx) error {
	response, err := handler.Storage.GetRule(c.Params("id"))
	if err != nil {
		return c.SendStatus(fiber.StatusNoContent)
	}
	return c.Status(fiber.StatusOK).JSON(response)
}

func (handler *Handler) GetPattern(c *fiber.Ctx) error {
	response, err := handler.Storage.GetPattern(c.Params("idr"))
	if err != nil {
		return c.SendStatus(fiber.StatusNoContent)
	}
	return c.Status(fiber.StatusOK).JSON(response)
}

func (handler *Handler) GetAllParameters(c *fiber.Ctx) error {
	response, err := handler.Storage.GetAllParameters(c.Params("idpat"))
	if err != nil {
		return c.SendStatus(fiber.StatusNoContent)
	}
	return c.Status(fiber.StatusOK).JSON(response)
}

func (handler *Handler) GetParameter(c *fiber.Ctx) error {
	response, err := handler.Storage.GetParameter(c.Params("id"))
	if err != nil {
		return c.SendStatus(fiber.StatusNoContent)
	}
	return c.Status(fiber.StatusOK).JSON(response)
}

func (handler *Handler) UpdateKb(c *fiber.Ctx) error {
	var request UpdateKbRequest
	if err := c.BodyParser(&request); err != nil {
		return c.SendStatus(fiber.StatusBadRequest)
	}
	if err := handler.Storage.UpdateKb(request); err != nil {
		return c.SendStatus(fiber.StatusBadRequest)
	}
	return c.SendStatus(fiber.StatusOK)
}

func (handler *Handler) UpdateRule(c *fiber.Ctx) error {
	var request UpdateRuleRequest
	if err := c.BodyParser(&request); err != nil {
		return c.SendStatus(fiber.StatusBadRequest)
	}
	if err := handler.Storage.UpdateRule(request); err != nil {
		return c.SendStatus(fiber.StatusBadRequest)
	}
	return c.SendStatus(fiber.StatusOK)
}

func (handler *Handler) UpdatePattern(c *fiber.Ctx) error {
	var request UpdatePatternRequest
	if err := c.BodyParser(&request); err != nil {
		return c.SendStatus(fiber.StatusBadRequest)
	}
	if err := handler.Storage.UpdatePattern(request); err != nil {
		return c.SendStatus(fiber.StatusBadRequest)
	}
	return c.SendStatus(fiber.StatusOK)
}

func (handler *Handler) UpdateParameters(c *fiber.Ctx) error {
	var request []map[string]string
	if err := c.BodyParser(&request); err != nil {
		return c.SendStatus(fiber.StatusBadRequest)
	}
	parametersChanges := make([]UpdateParameterRequest, len(request))
	for i, elem := range request {
		newParameter := UpdateParameterRequest{
			ID:           elem["id"],
			NewShortname: elem["new_shortname"],
			NewType:      elem["new_type"],
		}
		parametersChanges[i] = newParameter
	}
	if err := handler.Storage.UpdateParameters(parametersChanges); err != nil {
		return c.SendStatus(fiber.StatusBadRequest)
	}
	return c.SendStatus(fiber.StatusOK)
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
	viper.AddConfigPath("../configs")
	viper.SetConfigName("config")
	return viper.ReadInConfig()
}

func main() {
	if err := initConfig(); err != nil {
		logrus.Fatalf("error initializing configs: %s", err.Error())
	}
	db := DBConnection()
	defer db.Close()

	handler := &Handler{
		Storage: &MyDB{
			db,
		},
	}
	webApp := fiber.New()
	webApp.Use(cors.New())
	webApp.Get("/getkbs", handler.GetAllKbs)
	webApp.Get("/getkb/:id", handler.GetKb)
	webApp.Get("/getrules/:idkb", handler.GetAllRules)
	webApp.Get("/getrule/:id", handler.GetRule)
	webApp.Get("/getpattern/:idr", handler.GetPattern)
	webApp.Get("getparameters/:idpat", handler.GetAllParameters)
	webApp.Get("/getparameter/:id", handler.GetParameter)
	webApp.Put("/updatekb", handler.UpdateKb)
	webApp.Put("/updaterule", handler.UpdateRule)
	webApp.Put("/updatepattern", handler.UpdatePattern)
	webApp.Put("updateparameters", handler.UpdateParameters)
	logrus.Fatal(webApp.Listen(viper.GetString("port")))
}
