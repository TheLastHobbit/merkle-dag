package merkledag

// Hash to file
func Hash2File(store KVStore, hash []byte, path string, hp HashPool) []byte {
	// 根据hash和path， 返回对应的文件, hash对应的类型是tree
	obj, success := store.Get(hash)
	if success != nil {
		panic("Hash2File: hash not found")
	}

	// 获取了data的二进制文件
	// 转化为对象
	data, err := Decode(obj)

	if data.Links == nil {
	    //link为空，说明为文件
		return data.Data
	}
	else{
		// links不为空，则说明还是还不是叶子节点
		// 递归调用
		headPath, tailPath := splitPath(path)
		for _, link := range data.Links {
		    if  link.Name == headPath{
			// 递归调用
			Hash2File(store, link.Hash, path, hp)
			}
		}

	}

	return nil
}

//处理string类型的path
func splitPath(path string) (head, tail string) {
	parts := strings.Split(path, "/")
	if len(parts) > 1 {
		head = parts[0]
		tail = strings.Join(parts[1:], "/")
	} else {
		head = parts[0]
		tail = ""
	}
	return
}