package http_api

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/julienschmidt/httprouter"
	"github.com/nsqio/nsq/internal/lg"
)

// 在APIHandler外面套了一层
type Decorator func(APIHandler) APIHandler

type APIHandler func(http.ResponseWriter, *http.Request, httprouter.Params) (interface{}, error)

type Err struct {
	Code int
	Text string
}

func (e Err) Error() string {
	return e.Text
}

func acceptVersion(req *http.Request) int {
	if req.Header.Get("accept") == "application/vnd.nsq; version=1.0" {
		return 1
	}

	return 0
}

// 对返回值为string和[]byte的结果进行输出
func PlainText(f APIHandler) APIHandler {
	return func(w http.ResponseWriter, req *http.Request, ps httprouter.Params) (interface{}, error) {
		code := 200
		data, err := f(w, req, ps)
		if err != nil {
			code = err.(Err).Code
			data = err.Error()
		}
		switch d := data.(type) {
		case string:
			// 在这里就把数据写出去了
			w.WriteHeader(code)
			io.WriteString(w, d)
		case []byte:
			// 在这里就把数据写出去了
			w.WriteHeader(code)
			w.Write(d)
		default:
			// 在这里打出未知数据类型
			// TODO @limingji 这里的校验很强啊
			panic(fmt.Sprintf("unknown response type %T", data))
		}
		return nil, nil
	}
}

func V1(f APIHandler) APIHandler {
	return func(w http.ResponseWriter, req *http.Request, ps httprouter.Params) (interface{}, error) {
		data, err := f(w, req, ps)
		if err != nil {
			RespondV1(w, err.(Err).Code, err)
			return nil, nil
		}
		RespondV1(w, 200, data)
		return nil, nil
	}
}

// http version 1.0的返回值
func RespondV1(w http.ResponseWriter, code int, data interface{}) {
	var response []byte
	var err error
	var isJSON bool

	if code == 200 {
		// 首先将数据统一成bytes
		switch data.(type) {
		case string:
			response = []byte(data.(string))
		case []byte:
			response = data.([]byte)
		case nil:
			response = []byte{}
		default:
			isJSON = true
			response, err = json.Marshal(data)
			if err != nil {
				code = 500
				data = err
			}
		}
	}
	// 检查code
	if code != 200 {
		isJSON = true
		// 这个json.Marshal基本上没有时候会返回error吧
		response, _ = json.Marshal(struct {
			Message string `json:"message"`
		}{fmt.Sprintf("%s", data)})
	}
	//  如果是Json数据的话，则写入Content-Type的json类型
	if isJSON {
		w.Header().Set("Content-Type", "application/json; charset=utf-8")
	}
	w.Header().Set("X-NSQ-Content-Type", "nsq; version=1.0")
	// 写入状态码
	w.WriteHeader(code)
	// 写入response
	w.Write(response)
}

// 一个请求过来之后会依次经过f, ds...
func Decorate(f APIHandler, ds ...Decorator) httprouter.Handle {
	decorated := f
	// 在执行的时候会先执行f 然后依次执行每个ds？
	// 这就相当于给f 外面套了一层一层的壳子。然后在最终执行的时候是需要从里向外执行的
	// 在执行的时候会首先执行最后一个decorate ?
	for _, decorate := range ds {
		decorated = decorate(decorated)
	}
	return func(w http.ResponseWriter, req *http.Request, ps httprouter.Params) {
		decorated(w, req, ps)
	}
}

func V12(f APIHandler) APIHandler {
	return func(w http.ResponseWriter, req *http.Request, ps httprouter.Params) (interface{}, error) {
		data, err := f(w, req, ps)
		if err != nil {
			RespondV1(w, err.(Err).Code, err)
			return nil, nil
		}
		RespondV1(w, 200, data)
		return nil, nil
	}
}

// 通过log来记录所有的请求
func Log(logf lg.AppLogFunc) Decorator {
	return func(f APIHandler) APIHandler {
		return func(w http.ResponseWriter, req *http.Request, ps httprouter.Params) (interface{}, error) {
			start := time.Now()
			// 首先执行 APIHandler f，然后对f的耗时和状态码进行统计
			response, err := f(w, req, ps)
			elapsed := time.Since(start)
			status := 200
			if e, ok := err.(Err); ok {
				status = e.Code
			}
			// 在这里通过log info来进行记录
			logf(lg.INFO, "%d %s %s (%s) %s", status, req.Method, req.URL.RequestURI(), req.RemoteAddr, elapsed)
			return response, err
		}
	}
}

// 处理Panic
func LogPanicHandler(logf lg.AppLogFunc) func(w http.ResponseWriter, req *http.Request, p interface{}) {
	return func(w http.ResponseWriter, req *http.Request, p interface{}) {
		logf(lg.ERROR, "panic in HTTP handler - %s", p)
		Decorate(func(w http.ResponseWriter, req *http.Request, ps httprouter.Params) (interface{}, error) {
			return nil, Err{500, "INTERNAL_ERROR"}
		}, Log(logf), V1)(w, req, nil)
	}
}

// 处理未找到的方法
func LogNotFoundHandler(logf lg.AppLogFunc) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		Decorate(func(w http.ResponseWriter, req *http.Request, ps httprouter.Params) (interface{}, error) {
			return nil, Err{404, "NOT_FOUND"}
		}, Log(logf), V1)(w, req, nil)
	})
}

// 处理Method不对的情况 e.g. 要post给get
func LogMethodNotAllowedHandler(logf lg.AppLogFunc) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		Decorate(func(w http.ResponseWriter, req *http.Request, ps httprouter.Params) (interface{}, error) {
			return nil, Err{405, "METHOD_NOT_ALLOWED"}
		}, Log(logf), V1)(w, req, nil)
	})
}
