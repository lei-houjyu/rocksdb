rubble_grpc_srcs: 
	/root/bin/protoc --grpc_out=../rubble --cpp_out=../rubble  --plugin=protoc-gen-grpc=/root/bin/grpc_cpp_plugin rubble_kv_store.proto

clean:
	rm ../rubble/rubble_kv_store.*
.phony: clean