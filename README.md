# Scraping Engine
_updated November 5, 2020_

Overview: we're scraping all of Florida to build a comprehensive view of each property. In each county, we want to scrape the property data, the tax data for that property, as well as all of the Official Record Information (ORI) that the property offers. We want to scrape the data as far back as it goes (usually around 1960) up until today. We then want to setup a scraper to grab this information every single hour. We want to ensure we're getting as much data as possible to deliver a comprehensive report on the property. This includes all of the data fields as well as PDF's and drawings of the property. 

Requirements:
* AWS access for EC2, S3, and Lambda

Standards:
* Code should be written in Scrapy or another library that is similar in speed (don't use Selenium)
* When we're scraping all past data, we'll deploy on EC2 since it will scrape 24 hours a day 
* The scraper should be modified to get the latest data for today, that scraper can be deployed on Lambda
* Commit your code on a branch and submit a PR using the naming convention `<countyName-datatype>/<your name>`. For example: `miami-taxes/brody`


Outlined are the steps to get started scraping: 

1. Here is the where we keep track of counties and who is scraping what website. You can find the URL to be scraped [here](https://docs.google.com/spreadsheets/d/1tz46JDjERiINWZ3OmR5thW3_I5vgziC9BR3oG_q6Z7o/edit?ts=5fa322f3#gid=0)

2. The folio number (name disambigation below) is located in the Name Address Legal (NAL) file [here](https://www.dropbox.com/sh/46l2y2biw0efgtb/AABkpXSM_UzH9B6USsikPOhqa?dl=0) under the respective county. Refer to the County codes.
3. On the website's search field, use the folio number to search for the property. Retrieve all relevant information on a property, including PDF's and building diagrams/drawings.
4. Deploy your code on EC2 cluster
5. Upload your scraped data to this MongoDB collection: `mongodb+srv://hbg:**cluster0.uetxd.mongodb.net/`
using the <county name>.<tax/property/ori> standardization, where county name is the collection and the document type is another collection inside of it. 
6. If there are PDF documents that need to be saved, save them in the *pdw-database* S3 bucket using the same standardization, where county name is the first folder with the data type folder enclosed.
7. Submit a pull request to master tagging Broderick or Rahul




A Folio Number is a unique number that is assigned to each tract of land in a county by the Tax Assessor. 
They are also called: 
* Assessor's Parcel Number (APN)
* Parcel ID
* PID
* Tax ID
* Sidwell
* TID
* Parcel Control Number
* PCN
* Key Number
* TMS #

Uploading data to MongoDB Code Snippet:
https://gist.github.com/rrshenoy/4ed3612aeb628413b7b8e571948f3224


# County Codes
*These are your county codes with the corresponding county number and county name for the Name Address Legal (NAL) files.*

Under Folder NAL EACH COUNTY choose the corresponding File Number NAL11, NAL12, etc

For example, the document in [dropbox](https://www.dropbox.com/sh/46l2y2biw0efgtb/AABkpXSM_UzH9B6USsikPOhqa?dl=0) "NAL30F202001.csv" with NAL 30 would correspond to Franklin. 

```
11	Alachua
12	Baker
13	Bay
14	Bradford
15	Brevard
16	Broward
17	Calhoun
18	Charlotte
19	Citrus
20	Clay
21	Collier
22	Columbia
23	Miami-Dade
24	DeSoto
25	Dixie
26	Duval
27	Escambia
28	Flagler
29	Franklin
30	Gadsden
31	Gilchrist
32	Glades
33	Gulf
34	Hamilton
35	Hardee
36	Hendry
37	Hernando
38	Highlands
39	Hillsborough
40	Holmes
41	Indian River
42	Jackson
43	Jefferson
44	Lafayette
45	Lake
46	Lee
47	Leon
48	Levy
49	Liberty
50	Madison
51	Manatee
52	Marion
53	Martin
54	Monroe
55	Nassau
56	Okaloosa
57	Okeechobee
58	Orange
59	Osceola
60	Palm Beach
61	Pasco
62	Pinellas
63	Polk
64	Putnam
65	Saint Johns
66	Saint Lucie
67	Santa Rosa
68	Sarasota
69	Seminole
70	Sumter
71	Suwannee
72	Taylor
73	Union
74	Volusia
75	Wakulla
76	Walton
77	Washington
```
# county-scrapers
