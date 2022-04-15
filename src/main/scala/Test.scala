/**
 * Created by wangning on 2022/1/25 19:14.
 */
object Test {
  val doSeq: Seq[String] = Seq("AS","CC","CJH","CJY","CLC","CQQ","DB","DC","DD","DH","DJW","DW","DY","FLJ","FSJ","GDQ","HGY","HWT","LDH","LJ","LZA","MXM","MXX","PS","PSY","QCY","taya","TL","TYL","WQ","WR","WYY","XG","YJH","YL","YSJ","YY","ZB","ZDK","ZN","ZSY","ZXL","ZYL","ZYY","其他")

  def main(args: Array[String]): Unit = {
//    0 是提出人
//    1 是设计师

        val videoName = "99-0105-MDM-9比16-新SET3-真人-设计房间-idea-WB-WB03.mp4"
        val str = strExtract(videoName, 0, "-")
        println(str)
    println(doSeq.size)
  }

  def strExtract(videName: String, position: Int, splitRegex: String): String = {

    //99-1125-MDM-4比5-主流-SET3-跨品类主流-女性向-美妆类(美妆+换装)(短版)-idea-XG-QCY20
    val regex = "^[a-zA-Z]+".r
    val arr = videName.split("idea")
    println(arr.mkString(","))
    val result = if (arr.size == 2) {
      val idea = arr(1).split(splitRegex)
      println(idea.mkString(","))
      val buffer = idea.toBuffer
      if (idea.contains("")) {
        buffer.remove(0)
      }
      if (buffer.size == 1) {
        buffer += "其他"
      }
      regex.findFirstIn(buffer(position)).getOrElse("其他")
    } else {
      new RuntimeException("video_name 命名规则有误，具体videoName为:" + videName)
    }
    result.asInstanceOf[String].toUpperCase

    val value = result.asInstanceOf[String].toUpperCase
    println(value)
    if (doSeq != null && doSeq.contains(value)) {
      value
    } else {
      "其他"
    }
  }
}
