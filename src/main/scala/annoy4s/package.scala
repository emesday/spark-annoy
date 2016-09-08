import com.github.fommil.netlib.BLAS

package object annoy4s {
  val Zero = 0f
  val One = 1f
  val blas = BLAS.getInstance()

  def showUpdate(text: String, xs: Any*): Unit = Console.err.print(text.format(xs: _*))
}
