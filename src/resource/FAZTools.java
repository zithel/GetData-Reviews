package resource;

public class FAZTools
{
	public String TimeString(long seconds)
    {
	    String strTm = "";
	    if (seconds / 3600 > 0)
	    {
	    	strTm = (seconds / 3600) + "h ";
	    	seconds %= 3600;
	    }
	    if (seconds / 60 > 0)
	    {
	    	strTm += (seconds / 60) + "m ";
	    	seconds %= 60;
	    }
    	strTm += seconds + "s";
	    return strTm;
    }
}