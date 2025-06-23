
# Databricks Asset Bundle Deployment using Databricks CLI and VS Code
# Section 1
![image](https://github.com/user-attachments/assets/88534ff2-ce48-4328-9190-493ab2a0f969)

Please watch out my medium article to understand the steps I followed
---------------------------------------------------------------------------------------------------------------------



# https://medium.com/@bijumathewt/databricks-asset-bundle-deployment-using-databricks-cli-and-vs-code-a9519deba958  

# Section 2  - Process Order Data using Medallion 

All files are available in repository. Execution steps can follow from 1....

![image](https://github.com/user-attachments/assets/fd8e1eaf-c8c6-466c-bc8a-35c260433d4a)   


Json Data Format is here 

{"order_id":"ORD-78534","customer_id":"CUST-3080","product_id":"PROD-646","quantity":10,"price":499.02,"order_timestamp":"2025-06-23T19:23:09.161Z","status":"CANCELLED"}
{"order_id":"ORD-88093","customer_id":"CUST-6168","product_id":"PROD-567","quantity":9,"price":355.91,"order_timestamp":"2025-06-23T19:23:09.161Z","status":"PENDING"}
{"order_id":"ORD-84206","customer_id":"CUST-3602","product_id":"PROD-466","quantity":4,"price":273.94,"order_timestamp":"2025-06-23T19:23:09.161Z","status":"COMPLETED"}
{"order_id":"ORD-87743","customer_id":"CUST-5625","product_id":"PROD-506","quantity":2,"price":255.11,"order_timestamp":"2025-06-23T19:23:09.161Z","status":"COMPLETED"}
{"order_id":"ORD-61406","customer_id":"CUST-6899","product_id":"PROD-570","quantity":9,"price":453.24,"order_timestamp":"2025-06-23T19:23:09.161Z","status":"SHIPPED"}
{"order_id":"ORD-36705","customer_id":"CUST-4765","product_id":"PROD-556","quantity":4,"price":63.04,"order_timestamp":"2025-06-23T19:23:09.161Z","status":"COMPLETED"}
{"order_id":"ORD-70191","customer_id":"CUST-6502","product_id":"PROD-594","quantity":9,"price":244.56,"order_timestamp":"2025-06-23T19:23:09.161Z","status":"COMPLETED"}
{"order_id":"ORD-12211","customer_id":"CUST-6082","product_id":"PROD-597","quantity":1,"price":486.73,"order_timestamp":"2025-06-23T19:23:09.161Z","status":"CANCELLED"}
{"order_id":"ORD-97726","customer_id":"CUST-3627","product_id":"PROD-478","quantity":3,"price":192.96,"order_timestamp":"2025-06-23T19:23:09.161Z","status":"SHIPPED"}
{"order_id":"ORD-75194","customer_id":"CUST-4578","product_id":"PROD-576","quantity":6,"price":487.33,"order_timestamp":"2025-06-23T19:23:09.161Z","status":"PENDING"}
{"order_id":"ORD-54458","customer_id":"CUST-4667","product_id":"PROD-500","quantity":6,"price":17.24,"order_timestamp":"2025-06-23T19:23:09.161Z","status":"CANCELLED"}
{"order_id":"ORD-21510","customer_id":"CUST-5572","product_id":"PROD-578","quantity":10,"price":452.67,"order_timestamp":"2025-06-23T19:23:09.161Z","status":"COMPLETED"}


All external locations are pointing to containers in ADLS  

![image](https://github.com/user-attachments/assets/5fd7f3af-a474-4907-836c-b33699b90200)


I have used Unity Catalog to store delta tables.    

![image](https://github.com/user-attachments/assets/53bca8b1-e034-473b-8bb6-ddda84c5b27e)


![image](https://github.com/user-attachments/assets/31220aa6-2ad8-47a3-8175-9bf9cd933a6e)  
![image](https://github.com/user-attachments/assets/43e4a029-7aaa-4874-bca0-bfde57f7f784)  
![image](https://github.com/user-attachments/assets/ec1a16c8-1428-45ee-bd39-a48ef29aea75)  
![image](https://github.com/user-attachments/assets/631f97ae-370a-4c9a-968c-b0d529f91722)  







  


# Section 3 - DLT
![image](https://github.com/user-attachments/assets/83a3675c-9daf-4082-b730-860ebf107606)


![image](https://github.com/user-attachments/assets/2cb5a6f2-e6ed-404e-bea7-0f42bc1fae5e)


![image](https://github.com/user-attachments/assets/b65a75a1-d5fa-447d-9f88-bba0215f47ef)


![image](https://github.com/user-attachments/assets/1b8296df-8a15-41fb-aec6-f90c9ca4db63)


![image](https://github.com/user-attachments/assets/a9a4d24a-697f-45cc-8358-b5e0ba4f4c8c)


![image](https://github.com/user-attachments/assets/0ad38d88-3229-4d3f-b064-ddbcf355bdf3)

![image](https://github.com/user-attachments/assets/142b1326-8a24-4c43-94ba-b0b1334a0a5d)
