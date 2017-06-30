/*
 * list out bucket names, sizes, last modified
 */

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.net.URI;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.AWSCredentials;
// import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.profile.internal.BasicProfile;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.S3ClientOptions;
import com.amazonaws.services.s3.model.Bucket;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.regions.RegionUtils;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.amazonaws.regions.AwsProfileRegionProvider;
import com.amazonaws.regions.AwsRegionProvider;
import com.amazonaws.regions.DefaultAwsRegionProviderChain;
//import com.amazonaws.regions.InMemoryRegionImpl;
import com.amazonaws.ClientConfiguration;
import com.amazonaws.Protocol;

public class p4 {

    public static void main(String[] argv) throws IOException {

	String endpoint = null;
	String bucketname = null;
        AWSCredentials credentials = null;
	String prefix = null;
	String datetime_format_string = "yyyy-MM-dd'T'hh:mm:ss.SSS'Z'";
	int sequals;
	int maxkeys = 0;
	String profile_name = null;

	sequals = 0;
	for (String s: argv) {
	    switch (sequals) {
	    case 1:
		profile_name = s;
		sequals = 0;
		continue;
	    case 2:
		try {
		    maxkeys = Integer.parseInt(s);
		} catch (Exception e) {
		    throw new AmazonClientException(
			"can't parse integer: ", e);
		}
		sequals = 0;
		continue;
	    }
	    if (s.equals("--profile")) {
		    sequals = 1;
		    continue;
	    }
	    if (s.equals("--maxkeys")) {
		    sequals = 2;
		    continue;
	    }
	    if (bucketname != null)
		throw new AmazonClientException("Usage: p4 <bucketname>");
	    bucketname = s;
	}
	if (sequals != 0) {
	    throw new AmazonClientException("--profile: missing arg");
	}

//	ProfileCredentialsProvider pcprovider = new ProfileCredentialsProvider();
	AWSCredentialsProvider pcprovider = new DefaultAWSCredentialsProviderChain();
        try {
            credentials = pcprovider.getCredentials();
        } catch (Exception e) {
            throw new AmazonClientException(
		"error loading creds: ", e);
        }
	System.out.println("access_key = " + credentials.getAWSAccessKeyId());
	System.out.println("secret_key = " + credentials.getAWSSecretKey());
/*	endpoint = pcprovider.profile.getPropertyValue("endpoint"); */

	RegionUtils ru = new RegionUtils();

	ru.initialize();

	for (Region r: ru.getRegions()) {
                System.out.println("\n" + r.getName());
		String ep = r.getServiceEndpoint("s3");
		System.out.println("ep = " + ep);
		String dom = r.getDomain();
		System.out.println("dom = " + ep);
		boolean has_https = r.hasHttpsEndpoint("s3");
		System.out.println("has_https = " + (has_https ? "yes" : "no"));
	}


	AwsRegionProvider rpu;
	rpu = new AwsProfileRegionProvider();
	String region = rpu.getRegion();
	System.out.println("\nprofile-only.region = " + region);

//	if (profile_name != null)
//		rpu = new DefaultAwsRegionProviderChain(profile_name);
//	else
		rpu = new DefaultAwsRegionProviderChain();

	region = rpu.getRegion();

	System.out.println("\nprovider-chain.region = " + region);

	Region region_x = RegionUtils.getRegion(region);

	ClientConfiguration clientConfig = new ClientConfiguration();

	if (!region_x.hasHttpsEndpoint("s3"))
		clientConfig.setProtocol(Protocol.HTTP);

        AmazonS3 s3 = new AmazonS3Client(credentials, clientConfig);

//	String myregion_1 = new AwsProfileRegionProvider().getRegion();
//	s3.setRegion(myregion);
	s3.setRegion(region_x);
	s3.setS3ClientOptions(new S3ClientOptions().withPathStyleAccess(true));

//	System.exit(9);

        try {
	    ListObjectsRequest or = new ListObjectsRequest()
		.withBucketName(bucketname);
	    if (prefix != null)
		or.withPrefix(prefix);
	    if (maxkeys != 0)
		or.setMaxKeys(maxkeys);
            ObjectListing objectListing = s3.listObjects(or);
            for (S3ObjectSummary objectSummary : objectListing.getObjectSummaries()) {
		Date lastmod = objectSummary.getLastModified();
		String formatted_lastmod = new SimpleDateFormat(
		    datetime_format_string).format(lastmod);
                System.out.println(objectSummary.getKey() + "\t" +
		    objectSummary.getSize() + "\t" + formatted_lastmod);
            }
        } catch (AmazonServiceException ase) {
            System.out.println("AmazonServiceException");
            System.out.println("Error Message:    " + ase.getMessage());
            System.out.println("HTTP Status Code: " + ase.getStatusCode());
            System.out.println("AWS Error Code:   " + ase.getErrorCode());
            System.out.println("Error Type:       " + ase.getErrorType());
            System.out.println("Request ID:       " + ase.getRequestId());
//        } catch (AmazonClientException ace) {
 //           System.out.println("AmazonClientException");
  //          System.out.println("Error Message: " + ace.getMessage());
        }
    }
}
