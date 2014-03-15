telepath
========

System for mining Wikipedia Usage data to read our collective mind.

This software is an extension of the [Infovore](https://github.com/paulhoule/infovore) toolkit.

We have copied nearly
[4 TB of data](http://dumps.wikimedia.org/other/pagecounts-raw/) from the Wikimedia foundation,  a process
that took a whole month because of their slow server.  This data is available in the S3 bucket at
`s3://wikimedia-pagecounts//` and can be easily processed with the telepath software inside Amazon Elastic Map/Reduce.

This data costs several hundreds of dollars a month to store,  so we are
taking donations at <a href="https://www.gittip.com/paulhoule/">Gittip</a> to pay for the cloud resource cost.  Please take a moment to register at Gittip and make a small weekly donation.  If we reach our funding goal of $125 a week,  I can make the full data set freely available.

Learn more at the [wiki](https://github.com/paulhoule/telepath/wiki) 
