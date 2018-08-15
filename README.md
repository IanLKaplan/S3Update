# S3Update
A Java application that copies/updates files in an Amazon Web Services (AWS) Simple Storage System (S3) bucket 
with files from a local directory tree.  This code is targeted at copying website files from a directory on your local system to a publicly viewable S3 bucket.

AWS S3 is a web based storage system. Access to S3 takes place via HTTP. This allows S3 to host web content that is
(generally) static. Hosting static web sites on Amazon S3 costs far less that most alternatives.

Often the web content is developed on the local system and then downloaded to AWS S3. For any sizable web site, copying 
the files and directories can be time consuming, tedious and error prone (e.g., a file or directory can be mistakenly omitted).
When changes are made, the modified files must be copied over.

This application allows entire directory trees to be copied to AWS S3. Once the web site exists on S3 and local changes are
made, this applicaiton will copy over the files that have changed.

## Building a web site with S3

Amazon S3 content is web based (e.g., S3 content is available via HTTP). By default the content is not visible outside of 
the user's virtual private cloud. But the content can be made public. This allows static web sites to be created. 
Amazon provides a clear guide on how to setup a static website, using a custom domain.

[Example: Setting up a Static Website Using a Custom Domain](https://docs.aws.amazon.com/AmazonS3/latest/dev/website-hosting-custom-domain-walkthrough.html)

In this example to S3 buckets are created, example.com and www.example.com. The example.com bucket has a JSON 
permission record associated with it. The www.example.com bucket should be created as public, but it should not 
have a JSON record associated with it.
