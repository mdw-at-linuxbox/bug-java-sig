/*
 * generate presigned url
 */

import java.io.File;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.net.URI;
import java.net.URL;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.AWSCredentials;
// import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.profile.internal.BasicProfile;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.PutObjectRequest;
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

public class p7 {

    public static void Usage() {
	System.err.println("Usage: p7 bucket keyname expires");
	System.exit(9);
    }

    static Date convert_to_date(String s) {
	Date result = new Date();
	result.setTime(Date.parse(s));
//	System.err.println("sorry, need to implement freeform date parsing here!");
//	System.exit(9);
	SimpleDateFormat sf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSZ");
	System.out.println("date = " + sf.format(result));
	return result;
    }

    public static void main(String[] argv) throws IOException {

	String bucketName = null;
        AWSCredentials credentials = null;
	int sequals;
	String profile_name = null;
	String keyName = null;
	Date expires = null;
	URL url;

	sequals = 0;
	for (String s: argv) {
	    switch (sequals) {
	    case 1:
		profile_name = s;
		sequals = 0;
		continue;
	    }
	    if (s.equals("--profile")) {
		    sequals = 1;
		    continue;
	    }
            if (bucketName == null)
		bucketName = s;
	    else if (keyName == null)
		keyName = s;
	    else if (expires == null)
		expires = convert_to_date(s);
	    else
		Usage();
	}
	if (sequals != 0) {
	    System.err.println("--profile: missing arg");
	    Usage();
	}
	if (keyName == null) {
	    System.err.println("missing keyname");
	    Usage();
	}

	if (expires == null) {
		expires = new Date();
		expires.setTime(expires.getTime() + 86400L * 1000);
	}

//	SimpleDateFormat sf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSZ");
//	System.out.println("expires = " + sf.format(expires));

	AWSCredentialsProvider pcprovider = new DefaultAWSCredentialsProviderChain();
        try {
            credentials = pcprovider.getCredentials();
        } catch (Exception e) {
            throw new AmazonClientException(
		"error loading creds: ", e);
        }

	RegionUtils ru = new RegionUtils();

	ru.initialize();

	AwsRegionProvider rpu;
	rpu = new AwsProfileRegionProvider();
	String region = rpu.getRegion();
//	System.out.println("\nprofile-only.region = " + region);

//	if (profile_name != null)
//		rpu = new DefaultAwsRegionProviderChain(profile_name);
//	else
		rpu = new DefaultAwsRegionProviderChain();

	region = rpu.getRegion();

//	System.out.println("\nprovider-chain.region = " + region);

	Region region_x = RegionUtils.getRegion(region);

	ClientConfiguration clientConfig = new ClientConfiguration();

	if (!region_x.hasHttpsEndpoint("s3"))
		clientConfig.setProtocol(Protocol.HTTP);

        AmazonS3 s3 = new AmazonS3Client(credentials, clientConfig);

//	String myregion_1 = new AwsProfileRegionProvider().getRegion();
//	s3.setRegion(myregion);
	s3.setRegion(region_x);
	s3.setS3ClientOptions(S3ClientOptions.builder()
		.setPathStyleAccess(true)
		.setPayloadSigningEnabled(true)
		.build());

//	System.exit(9);

        try {
	    url = s3.generatePresignedUrl(bucketName, keyName, expires);
            System.out.println(url);
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
