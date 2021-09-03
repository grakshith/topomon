package server

import (
	"net/http"

	"github.com/gorilla/mux"
)

func redirectToIndex(w http.ResponseWriter, r *http.Request) {
	http.Redirect(w, r, "/dashboard/", http.StatusFound)
}

func ConfigureURLS(handler *ConnectionHandler, pClient *PromClient) *mux.Router {
	router := mux.NewRouter()
	router.HandleFunc("/ws", handler.ServeHTTP)
	router.HandleFunc("/", redirectToIndex)
	router.PathPrefix("/dashboard/").Handler(http.StripPrefix("/dashboard/", http.FileServer(http.Dir("."))))
	router.HandleFunc("/metrics", pClient.metricsAdd)

	return router
}
