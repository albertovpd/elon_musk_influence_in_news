# Elon Musk infuence in world news.


Can we monitor a public person across the world news media, analyse the sentiment associated to each article and even having a glance to them?Yes. 

Thanks to **BigQuery** and **The Gdelt Project**.
Here is the Elon Reeve Musk and his top companies dashboard: Teslamotors, SpaceX, OpenAI, Neuralink, the Boring Company, Paypal, Solarcity. 

*This project was meant to be automated and monthly updated, but I already know how to do that (it's always the same), so I stopped it.*

The procedure is the following:
- Create a table with the content you want (**1st_query.sql**)
- Perform another query with the difference of days wanted, use *append* in scheduling options to update the table created in first place with new info (**2nd_query.sql**). 

You can see another example here => https://github.com/albertovpd/automated_etl_google_cloud-social_dashboard*

- Dashboard => https://datastudio.google.com/reporting/4578bcda-00a9-46e5-aacc-d773bfb360c2



![alt](pics/elon_project.gif)

