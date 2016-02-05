
val range=(1 to 1000)
val naturals=range.map(_.toDouble/1000).toArray

val s=new Hasher(naturals)


Hasher.r(100,1000L)