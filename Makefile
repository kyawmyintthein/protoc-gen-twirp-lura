build:
	go build -o protoc-gen-twirplura .
gen:
	protoc --plugin protoc-gen-twirplura --twirplura_out=. --go_out=. example.proto