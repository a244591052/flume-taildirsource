#Flume1.7.0 taildirSource bug 修复——文件重命名后重复采集数据
相对于flume 1.6.0 版本，flume 1.7.0 推出了 taildirSource 组件，通过tail 监控正则表达式
匹配目录下的所有文件，实现断点续传。

##发现taildirSource重复采集数据问题
flume 1.7.0 官方的 taildirSource 对于log4j 日志的监控会有bug。
因为log4j 日志会自动切分，可以按天或者按小时进行切分，log4j 切分日志其实就是新建
一个文件，然后修改原来的日志文件名称。但是 taildirSource 组件是不支持修改文件名称
的，如果文件被修改名称了，那么taildirSource 会认为是一个新的文件，就会重新读取该文
件中的数据，这就导致了日志文件重读，造成数据重复采集问题。

##修改taildirSource解决问题
通过阅读源码发现里面存在bug，只需要修改几处源码就可以解决这个bug问题。
首先从flume 官方下载flume1.7 源码，找到这个文件
`apache-flume-1.7.0-src\flume-ng-sources\flume-taildir-source\src\main\java\org\apache\flume\source\taildir\ReliableTaildirEventReader.java`

1. 修改updateTailFiles方法
```
/**
   * Update tailFiles mapping if a new file is created or appends are detected
   * to the existing file.
   * 不断扫描文件目录，更新tailFiles的映射
   * 此处需要修改源码
   */
  public List<Long> updateTailFiles(boolean skipToEnd) throws IOException {
    updateTime = System.currentTimeMillis();
    //每次扫描整个文件，并返回所有inode集合
    List<Long> updatedInodes = Lists.newArrayList();

    //循环filegroups（f1,f2...）对应的不同目录
    for (TaildirMatcher taildir : taildirCache) {
      Map<String, String> headers = headerTable.row(taildir.getFileGroup());

      //循环其中一个f1（包含该目录下，与该文件模式相匹配的所有文件）
      for (File f : taildir.getMatchingFiles()) {
        //inode文件唯一标示（不因文件的名称而改变）
        long inode = getInode(f);
        //上面已经定义了一个tailFiles map，刚开始会为空
        TailFile tf = tailFiles.get(inode);

        //*****************delete the line by licheng start**********
        /**
         * 如果加上!tf.getPath().equals(f.getAbsolutePath()判断，会有bug
         * 当目录下的文件名修改后，tf.getPath()与f.getAbsolutePath()路径不一致，
         * 会进入if子句，会造成重新消费采集该文件，因为此时startPos默认为0，
         * 又从0开始采集
         */
        //if (tf == null || !tf.getPath().equals(f.getAbsolutePath())) {
        //*****************delete the line by licheng end**********
        if (tf == null) {
          //skipToEnd如果没有记录读取位置时，是否跳过文件结尾，默认false
          long startPos = skipToEnd ? f.length() : 0;
          //根据f具体文件生成TailFile
          tf = openFile(f, headers, inode, startPos);
        } else {
          boolean updated = tf.getLastUpdated() < f.lastModified();
          //如果最后一个更新落后最后一次修改
          if (updated) {
            if (tf.getRaf() == null) {
              tf = openFile(f, headers, inode, tf.getPos());
            }

            if (f.length() < tf.getPos()) {
              logger.info("Pos " + tf.getPos() + " is larger than file size! "
                  + "Restarting from pos 0, file: " + tf.getPath() + ", inode: " + inode);
              tf.updatePos(tf.getPath(), inode, 0);
            }
          }

          //***************modify by licheng begin*****************
          //如果同一个文件的文件名称修改后，重新生成tf
          if (!tf.getPath().equals(f.getAbsolutePath())) {
            tf = openFile(f, headers, inode, tf.getPos());
          }
          //**************modify by licheng end*********************

          tf.setNeedTail(updated);
        }
        //添加inode与文件的映射关系
        tailFiles.put(inode, tf);
        //添加所有文件的inode
        updatedInodes.add(inode);
      }
    }
    return updatedInodes;
  }
```

2. 修改ReliableTaildirEventReader 构造方法里面的loadPositionFile(positionFilePath)方法。
```
    //根据inode获取本地目录下的TailFile
    TailFile tf = tailFiles.get(inode);
    //modify by licheng
    //如果tf不为空并且tf与f不一致，如果名称修改了，会造成inode pos数据不更新
    //if (tf != null && tf.updatePos(path, inode, pos)) {
    ////如果tf不为空，这里将tf与f保持一致（都传入tf，所以肯定一致）
    if (tf != null && tf.updatePos(tf.getPath(), inode, pos)) {
      //更新inode与tf映射
      tailFiles.put(inode, tf);
    } else {
      logger.info("Missing file: " + path + ", inode: " + inode + ", pos: " + pos);
    }
```

##Flume项目打包编译
1. 通过mvn package 对上述两个模块进行源码编译生成flume-taildirsource.jar
2. 将flume-taildirsource.jar 上传到flume lib 目录下即可生效

