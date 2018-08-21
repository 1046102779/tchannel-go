该typed库主要有ReadBuffer, WriteBuffer用于读写网络数据的payload中transport headers; 并提供了小内存io.Reader流读取数据字段

其中ReadBuffer用于读取网络数据, 提供了一些功能：

1. 并通过其中的属性Buffer和Remaining存储原始数据和未读数据；
2. 并提供了读取1字节、2字节、4字节、8字节和N字节的bytes流；
3. 提供读取tchannel协议规范中的payload部分transport headers字段数据, ReadLenXXString方法
4. 重置ReadBuffer和FillFrom读取n字节到io.Reader
5. 提供初始化ReadBuffer的可读内存区域, 也就是payload数据部分

WriteBuffer用于写入网络数据，提供一些功能：

1. 提供初始化WriteBuffer的空闲内存空闲，也就是写入payload部分的transport headers
2. 提供了写入1字节、2字节、4字节、8字节和N字节的bytes流；
3. 提供了写入tchannel协议规范中的payload部分transport headers字段数据，WriteLenXXString方法;
4. 提供了获取1字节、2字节、4字节、8字节和N字节的空闲内存引用；
5. 把已写入的内存数据，拷贝到io.Writer中;
6. 重置WriteBuffer的所有内存空间为空闲空间;

Reader小内存读取io.Reader数据字段，它采用临时对象池分配内存，提供了一些功能：

1. 提供了初始化Reader实例的方法，最大一次读取32字节的数据;
2. 提供了从io.Reader中读取2字节、N字节的方法；
3. 提供了读取payload中的transport header中2字节数据字段的方法。ReadLen16String, 先读取占用空间的数据长度，再根据长度读取实际数据；
4. 释放临时对象Reader
