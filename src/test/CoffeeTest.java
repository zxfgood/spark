package test;

public class CoffeeTest {
    public static void main(String[] args) {
        double[] [] x=new double[1][4];
        x[0][0]=1.0;
        x[0][1]=2.0;
        x[0][2]=3.0;
        x[0][3]=4.0;
        double[] y=new double[4];
        y[0]=5.0;
        y[1]=6.0;
        y[2]=6.0;
        y[3]=6.0;
        for (int s=0;s<1;s++){
            Double coeff=Coeff.coeff(x[s],y);
            System.out.println("第"+s+"个自变量与因变量的相关系数："+coeff);
        }
    }
}
