
协议帧读写解析网络数据包相关的文件, 包括: message.go, frame.go与typed目录下的所有文件

通过reqres.go、fragmenting_reader.go与fragmenting_writer.go三个文件，我们就完全知道了整个协议帧，包括协议帧的分片传输，读写解析和截取完整协议帧的所有过程, 并且还把ping req、call req新建立的msg id，然后通过message exchange等待处理和传送一个rpc调用完整流程的后续处理。

对上面的过程深刻理解，大家以后对协议的定义和读写解析是非常有帮助的。以及状态的扭转，协议帧的传送方式等等
