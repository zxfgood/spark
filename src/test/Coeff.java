package test;
/**
 * @描述：计算相关系数
 * @作者：胡荣、陈仁群改编
 * @日期：2018年3月1日
 */
public class Coeff {
	private Coeff(){}
	static double CORR = 0.0;
	public static double coeff(double[] xList,double[] yList){
		double numerator = calcuteNumerator(xList, yList);  
        double denominator = calculateDenominator(xList, yList);  
        CORR = numerator/denominator;
		return CORR;		
	}
	private static double calculateDenominator(double[] xList,double[] yList) {  
        int size = xList.length;  
        double xAverage = 0.0;  
        double yAverage = 0.0;  
        double xException = 0.0;  
        double yException = 0.0;  
        double temp = 0.0;  
        for(int i=0;i<size;i++){  
            temp += xList[i];  
        }  
        xAverage = temp/size;  
        
        temp = 0.0; 
        for(int i=0;i<size;i++){  
            temp += yList[i];  
        }  
        yAverage = temp/size;  
          
        for(int i=0;i<size;i++){  
            xException += Math.pow(xList[i]-xAverage,2);  
            yException += Math.pow(yList[i]-yAverage, 2);  
        }  
        //calculate denominator of   
        return Math.sqrt(xException*yException);  
    }
	private static double calcuteNumerator(double[] xList, double[] yList) {  
        double result =0.0;  
        double xAverage = 0.0;  
        double temp = 0.0;  
          
        int xSize = xList.length;  
        for(int x=0;x<xSize;x++){  
            temp += xList[x];  
        }  
        xAverage = temp/xSize;  
          
        double yAverage = 0.0;  
        temp = 0.0;  
        int ySize = yList.length;  
        for(int x=0;x<ySize;x++){  
            temp += yList[x];  
        }  
        yAverage = temp/ySize;  
          
        //double sum = 0.0;  
        for(int x=0;x<xSize;x++){  
            result+=(xList[x]-xAverage)*(yList[x]-yAverage);  
        }  
        return result;  
    }
}
