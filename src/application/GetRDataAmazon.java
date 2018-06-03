package application;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.Vector;
import javax.swing.JOptionPane;
// Third-party
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
// Custom
import resource.*;

public class GetRDataAmazon
{
	public static void main(String[] args) throws Exception
	{
		AmazonResult nwAmznRslt, prvsRslt = null;
		AmazonReview amznRvw;
		byte tmpByt1 = 0, tmpByt2 = 0, cntAmznRqsts = 0;
		Connection sqlCnnct;
		Document objHTML;
		LocalDateTime tmStrtMn, tmStrtLstng, tmStrtPrdct;
		Statement sqlStmnt1, sqlStmnt2;
		ResultSet sqlRsltSt;
		Vector<AmazonResult> vctrRslts = new Vector<AmazonResult>(0);
		String tmpStr = null, itmNm = null;
		FAZTools myTools = new FAZTools();
		
		try
		{
		// *** PRELIMINARY SETUP ***
			tmStrtMn = LocalDateTime.now();
			Class.forName("com.mysql.jdbc.Driver");
			sqlCnnct = DriverManager.getConnection("jdbc:mysql://localhost:3306/zithelMaster","fjawed","password");
			sqlStmnt1 = sqlCnnct.createStatement();
			sqlStmnt2 = sqlCnnct.createStatement();

			// Construct DB structure
			sqlStmnt1.execute("use zithelMaster;");
			sqlStmnt1.execute("create table if not exists mstr_products (ID varchar(10) primary key, BRAND_NAME text not null, NAME text not null, PRODUCT_TYPE varchar(10) not null, PAGE_URL text, IMAGE_URL text, IS_ACTIVE bit not null, PROCESSED_IN varchar(10));");
			sqlStmnt1.execute("create table if not exists cch_amazon_listings (ASIN varchar(10) primary key, PRODUCT_ID varchar(10) not null, LISTING_NAME text not null, REVIEWS_PAGE_URL text not null, REVIEW_COUNT int4 not null, PROCESSED_IN varchar(10));");
			sqlStmnt1.execute("create table if not exists mstr_amazon_reviews (ID varchar(15) primary key, ASIN varchar(10) not null, STARS int1 not null, REVIEWED_ON datetime not null, REVIEW_TITLE text not null, REVIEW_TEXT text not null, VOTED_HELPFUL int4 not null, CUSTOMER_REVIEW_URL text not null);");

		// *** GETTING INFORMATION FOR PRODUCTS ***
			sqlRsltSt = sqlStmnt1.executeQuery("select a.ID, a.BRAND_NAME, a.NAME, b.AMAZON_CATEGORY_TYPE, a.PROCESSED_IN, a.LAST_UPDATED from mstr_products as a left join mstr_product_type as b on a.PRODUCT_TYPE = B.ID where a.IS_ACTIVE = 1 and (a.LAST_UPDATED is null or DateDiff(a.LAST_UPDATED, CURRENT_DATE) > 15);");
			while (sqlRsltSt.next())
			{
				itmNm = "https://www.amazon.com/s?rh=" + sqlRsltSt.getString(4) + "&field-keywords=" + (sqlRsltSt.getString(2) + " " + sqlRsltSt.getString(3)).replace(" ",  "+").toLowerCase();
				// Purge stale records of listings and reviews
				vctrRslts.removeAllElements();
				sqlStmnt2.execute("delete from cch_amazon_listings where PRODUCT_ID='" + sqlRsltSt.getString(1) + "';");
				sqlStmnt2.execute("delete from mstr_amazon_reviews where ASIN in(select ASIN from cch_amazon_listings where PRODUCT_ID='" + sqlRsltSt.getString(1) + "');");
				tmStrtPrdct = LocalDateTime.now();
            	try
            	{
            		// Wait for 5 seconds after every 5 requests to Amazon
            		if(cntAmznRqsts > 5)
            		{
            			Thread.sleep(3000);
        				cntAmznRqsts = 0;
            		}
    				objHTML = Jsoup.connect("https://www.amazon.com/s/ref=sr_nr_n_1?rh=" + sqlRsltSt.getString(4) + "&field-keywords=" + (sqlRsltSt.getString(2) + " " + sqlRsltSt.getString(3)).replace(" ",  "+").toLowerCase() + "&ie=UTF8").timeout(5000).get();
    				cntAmznRqsts++;
            	}
            	catch (Exception e)
            	{
    				objHTML = Jsoup.connect("https://www.amazon.com/s?rh=" + sqlRsltSt.getString(4) + "&field-keywords=" + (sqlRsltSt.getString(2) + " " + sqlRsltSt.getString(3)).replace(" ",  "+").toLowerCase()).timeout(10000).get();
    				cntAmznRqsts++;
            	}
            	// No results found for the item
            	if(objHTML.getElementById("apsRedirectLink") != null || objHTML.getElementById("didYouMean") != null)
            		continue;
				// Evaluate result listings
            	if(objHTML.getElementById("s-results-list-atf") != null)
            	{
					for(Element lpElmnt : objHTML.getElementById("s-results-list-atf").children())
					{
						// Score listings based on word matches with the Product name
						if (lpElmnt.select("h5").text().contains("Sponsored") || lpElmnt.select("h5").text().contains("Ad"))
							continue;
						tmpByt1 = 0;
						tmpByt2 = 0;
						for (String lpStr1 : (sqlRsltSt.getString(2) + " " + sqlRsltSt.getString(3)).toLowerCase().split(" "))
				            for (String lpStr2 : lpElmnt.select("h2").text().toLowerCase().split(" "))
				            {
				            	// Only check for name in the first part of the listing; 3 words more than the product name
				            	//tmpByt2++;
				            	//if (tmpByt2 > (sqlRsltSt.getString(2) + " " + sqlRsltSt.getString(3)).toLowerCase().split(" ").length + 3)
				            		//break;
				                if (!(lpStr2.contains("(") | lpStr2.contains(")") | lpStr2.contains("/") | lpStr2.contains("\\") | lpStr2.contains("-") | lpStr2.contains("\"")))
				                	if (lpStr1.equals(lpStr2))
				                		tmpByt1++;
				            }
	
						// Register listings if matches are strong
			            if (tmpByt1 == (sqlRsltSt.getString(2) + " " + sqlRsltSt.getString(3)).split(" ").length)
			            {
			            	if(!lpElmnt.select("a.a-size-small.a-link-normal.a-text-normal").isEmpty())
			            	{
			            		nwAmznRslt = new AmazonResult(lpElmnt, sqlRsltSt.getString(1));
			
					            // Rank candidates on number of reviews
					            for (tmpByt1 = 0 ; tmpByt1 < vctrRslts.size() - 1 & vctrRslts.size() > 1; tmpByt1++)
					            {
					            	prvsRslt = (AmazonResult)vctrRslts.elementAt(tmpByt1);
					                if (nwAmznRslt.cntRvws > prvsRslt.cntRvws)
					                	prvsRslt.rnkLstng++;
					                else
					                	nwAmznRslt.rnkLstng++;
					            }
					            vctrRslts.addElement(nwAmznRslt);
	
					            // Insert listing in MySQL DB
					            try
					            {
					            	sqlStmnt2.execute("insert into cch_amazon_listings (ASIN,PRODUCT_ID,LISTING_NAME,REVIEWS_PAGE_URL,REVIEW_COUNT) values ('" + nwAmznRslt.ASIN + "','" + nwAmznRslt.idPrdct + "','" + nwAmznRslt.nmLstng + "','" + nwAmznRslt.urlPrdct + "'," + nwAmznRslt.cntRvws + ");");
					            }
					            catch (Exception e) {}
					            //sqlStmnt2.execute("delete from cch_amazon_listings where ASIN='" + nwAmznRslt.ASIN + "';");
			            	}
			            }
					}
		
		        // Retrieve reviews from top listings
					for (tmpByt1 = 0 ; tmpByt1 < vctrRslts.size(); tmpByt1++)
					{
						if (vctrRslts.elementAt(tmpByt1).cntRvws > 0)
						{
				            tmStrtLstng = LocalDateTime.now();
				            tmpStr = vctrRslts.elementAt(tmpByt1).urlPrdct + "/ref=cm_cr_arp_d_paging_btm_1?ie=UTF8&reviewerType=avp_only_review&pageSize=50&sortBy=recent&pageNumber=1";
				            tmpByt2 = 1;
			                do
			                {
			                	try
			                	{
			                		// Wait for 5 seconds after every 5 requests to Amazon
			                		if(cntAmznRqsts > 5)
			                		{
			                			Thread.sleep(1000);
			            				cntAmznRqsts = 0;
			                		}
			                		objHTML = Jsoup.connect(tmpStr).timeout(6000).get();
			        				cntAmznRqsts++;
			                	}
			                	catch (Exception e)
			                	{
			                		objHTML = Jsoup.connect(tmpStr).timeout(6000).get();
			        				cntAmznRqsts++;
			                	}
				                // Get Review
				                for (Element lpElmnt : objHTML.getElementById("cm_cr-review_list").children())
				                {
				                    if (lpElmnt.attr("class").equals("a-section review"))
				                    {
				                        amznRvw = new AmazonReview(lpElmnt);
				                        try
				                        {
				                        	sqlStmnt2.execute("INSERT INTO mstr_amazon_reviews (ID,PRODUCT_ID,ASIN,STARS,REVIEWED_ON,REVIEW_TITLE,REVIEW_TEXT,VOTED_HELPFUL,CUSTOMER_REVIEW_URL) VALUES ('" + 
				                        		amznRvw.idReview + "','" + sqlRsltSt.getString(1) + "','" + vctrRslts.elementAt(tmpByt1).ASIN + "'," + amznRvw.stars + ",'" + amznRvw.datePublish + "','" + amznRvw.title + "','" + 
			                        			amznRvw.body + "'," + amznRvw.countHelpful + ",'" + "https://www.amazon.com/gp/customer-reviews/" + amznRvw.idReview + "');");
				                        }
				                        catch (Exception e) {}
				                    }
				                }
			                // Get Next page info
				                if (objHTML.getElementById("cm_cr-pagination_bar") != null && objHTML.getElementById("cm_cr-pagination_bar").child(0).child(objHTML.getElementById("cm_cr-pagination_bar").child(0).children().size() - 1).attr("class").equals("a-last"))
				                {
				                    tmpStr = tmpStr.replace("pageNumber=" + tmpByt2, "pageNumber=" + (tmpByt2 + 1)).replace("cm_cr_getr_d_paging_btm_" + tmpByt2, "cm_cr_getr_d_paging_btm_" + (tmpByt2 + 1));
				                    tmpByt2++;
				                }
				                else
				                    break;
			                }
			                while (true);
			                sqlStmnt2.execute("UPDATE cch_amazon_listings SET PROCESSED_IN = '" + myTools.TimeString(tmStrtLstng.until(LocalDateTime.now(), ChronoUnit.SECONDS)) + "' WHERE ASIN = '" + vctrRslts.elementAt(tmpByt1).ASIN + "';");
			            }
	            	}
				}
                sqlStmnt2.execute("UPDATE mstr_products SET PROCESSED_IN = '" + myTools.TimeString(tmStrtPrdct.until(LocalDateTime.now(), ChronoUnit.SECONDS)) + "', LAST_UPDATED = '" + LocalDateTime.now().toString() + "' WHERE BRAND_NAME = '" + sqlRsltSt.getString(2) + "' AND NAME = '" + sqlRsltSt.getString(3) + "';");
            }
			sqlCnnct.close();
	        JOptionPane.showMessageDialog(null, "Data collation completed in " + myTools.TimeString(tmStrtMn.until(LocalDateTime.now(), ChronoUnit.SECONDS)), "Zithel data assistant", JOptionPane.INFORMATION_MESSAGE);
		}
		catch (Exception e)
		{
			System.out.println(itmNm);
			e.printStackTrace();
		}
	}
}

class AmazonResult
{
	byte rnkLstng;
	long cntRvws;
	String ASIN, idPrdct, nmLstng, urlPrdct;
	
	AmazonResult(Element elementResult, String idProduct)
	{
		ASIN = elementResult.attr("data-ASIN");
        idPrdct = idProduct;
        nmLstng = elementResult.select("h2").text();
        urlPrdct = "https://www.amazon.com/product-reviews/" + ASIN;
        if (elementResult.select("span[name='" + ASIN + "']").size() > 0)
        	cntRvws = Long.parseLong(elementResult.select("span[name='" + ASIN + "']").get(0).nextElementSibling().text().replaceAll(",", ""));
        else
        	cntRvws = 0;
        rnkLstng = 1;
	}
}

final class AmazonReview
{
	String body, countHelpful, datePublish, idReview, stars, title;
	private SimpleDateFormat lngDtFrmt = new SimpleDateFormat("MMMM d, yyyy"), shrtDtFrmt = new SimpleDateFormat("yyyy/MM/dd");
	
	AmazonReview(Element elementReview)
	{
		idReview = elementReview.attr("id");
		title = elementReview.select("[data-hook='review-title']").text().replaceAll("[^\\x00-\\x7F]", "").replace("'", "''").replace("\"", "''");
		stars = elementReview.select("[data-hook='review-star-rating']").text().split(" ")[0];
		try
		{
			datePublish = shrtDtFrmt.format(lngDtFrmt.parse(elementReview.select("[data-hook='review-date']").text().replace("on ", "")));
		}
		catch (ParseException e)
		{
			datePublish = "";
		}
		body = elementReview.select("[data-hook='review-body']").text().replaceAll("[^\\x00-\\x7F]", "").replace("\\n", "").replace("'", "''").replace("\"", "''");
    	countHelpful = "0";
        if (elementReview.select("[data-hook='helpful-vote-statement']").text().contains("found this helpful."))
        {
            if (elementReview.select("[data-hook='helpful-vote-statement']").text().trim().startsWith("One person"))
            	countHelpful = "1";
            else
            	countHelpful = elementReview.select("[data-hook='helpful-vote-statement']").text().trim().replace(" people found this helpful.", "").replace(",", "");
        }
	}
}