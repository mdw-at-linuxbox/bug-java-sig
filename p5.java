/*
 * list out bucket names, sizes, last modified
 */

import java.io.File;
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

public class p5 {

    public static void Usage() {
	System.err.println("Usage: p5 filename bucket object");
	System.exit(9);
    }

    public static void main(String[] argv) throws IOException {

	String bucketName = null;
        AWSCredentials credentials = null;
	int sequals;
	String profile_name = null;
	String uploadFilename = null;
	String keyName = null;

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
	    if (uploadFilename == null)
		uploadFilename = s;
            else if (bucketName == null)
		bucketName = s;
	    else if (keyName == null)
		keyName = s;
	    else
		Usage();
	}
	if (sequals != 0) {
	    throw new AmazonClientException("--profile: missing arg");
	}

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
	    File file = new File(uploadFilename);
	    PutObjectRequest pr = new PutObjectRequest(bucketName, keyName, file);
	    s3.putObject(pr);
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
