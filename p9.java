/*
 * copy object using "multipart upload" from another object
 * http://docs.aws.amazon.com/AmazonS3/latest/dev/CopyingObjctsUsingLLJavaMPUapi.html
 */

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
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
import com.amazonaws.services.s3.model.AbortMultipartUploadRequest;
import com.amazonaws.services.s3.model.CompleteMultipartUploadRequest;
import com.amazonaws.services.s3.model.InitiateMultipartUploadRequest;
import com.amazonaws.services.s3.model.InitiateMultipartUploadResult;
import com.amazonaws.services.s3.model.PartETag;
import com.amazonaws.services.s3.model.UploadPartRequest;
import com.amazonaws.services.s3.model.GetObjectMetadataRequest;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.CopyPartRequest;
import com.amazonaws.services.s3.model.CopyPartResult;
import com.amazonaws.regions.AwsProfileRegionProvider;
import com.amazonaws.regions.AwsRegionProvider;
import com.amazonaws.regions.DefaultAwsRegionProviderChain;
//import com.amazonaws.regions.InMemoryRegionImpl;
import com.amazonaws.ClientConfiguration;
import com.amazonaws.Protocol;

public class p9 {

    public static void Usage() {
	System.err.println("Usage: p9 source-bucket source-object dest-bucket dest-object");
	System.exit(9);
    }

    public static void main(String[] argv) throws IOException {

	String destinationBucket = null;
        AWSCredentials credentials = null;
	int sequals;
	String profile_name = null;
	String sourceBucket = null;
	String sourceKey = null;
	String destinationKey = null;
	long partSize = 5*1024*1024;

	sequals = 0;
	for (String s: argv) {
	    switch (sequals) {
	    case 1:
		profile_name = s;
		sequals = 0;
		continue;
	    case 2:
		try {
		    partSize = Long.parseLong(s);
		} catch (Exception e) {
		    throw new AmazonClientException(
			"can't parse long: ", e);
		}
		sequals = 0;
		continue;
	    }
	    if (s.equals("--partsize")) {
		    sequals = 2;
		    continue;
	    }
	    if (s.equals("--profile")) {
		    sequals = 1;
		    continue;
	    }
	    if (sourceBucket == null)
		sourceBucket = s;
            else if (sourceKey == null)
		sourceKey = s;
            else if (destinationBucket == null)
		destinationBucket = s;
	    else if (destinationKey == null)
		destinationKey = s;
	    else
		Usage();
	}
	if (sequals != 0) {
	    throw new AmazonClientException("--profile: missing arg");
	}
	if (sourceBucket == null) {
	    System.err.println("missing source-bucket");
	    Usage();
	}
	if (sourceKey == null) {
	    System.err.println("missing source-key");
	    Usage();
	}
	if (destinationBucket == null) {
	    System.err.println("missing destination-bucket");
	    Usage();
	}
	if (destinationKey == null) {
	    System.err.println("missing destination-key");
	    Usage();
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

	    GetObjectMetadataRequest mr = new
		GetObjectMetadataRequest (sourceBucket, sourceKey);
	    ObjectMetadata mdr = s3.getObjectMetadata(mr);
	    long objectSize = mdr.getContentLength();

	    List<PartETag> partETags = new ArrayList<PartETag>();
	    InitiateMultipartUploadRequest ir = new
		InitiateMultipartUploadRequest(destinationBucket, destinationKey);
	    InitiateMultipartUploadResult irr =
		s3.initiateMultipartUpload(ir);
	    long bytePosition = 0, thispartsize;
	    for (int i = 1; bytePosition < objectSize; ++i) {
		thispartsize = Math.min(partSize, (objectSize - bytePosition));
		CopyPartRequest cpr = new CopyPartRequest()
		    .withDestinationBucketName(destinationBucket)
		    .withDestinationKey(destinationKey)
		    .withSourceBucketName(sourceBucket)
		    .withSourceKey(sourceKey)
		    .withUploadId(irr.getUploadId()).withPartNumber(i)
		    .withFirstByte(bytePosition)
		    .withLastByte(bytePosition + thispartsize - 1);
		CopyPartResult cprr = s3.copyPart(cpr);
		partETags.add(new PartETag(cprr.getPartNumber(), cprr.getETag()));
		bytePosition += thispartsize;
	    }
	    CompleteMultipartUploadRequest cr = new
		CompleteMultipartUploadRequest(destinationBucket, destinationKey,
		irr.getUploadId(), partETags);
	    s3.completeMultipartUpload(cr);
        } catch (AmazonServiceException ase) {
// s3.abortMultipartUpload(new AbortMultipartUploadRequest(
// destinationBucket, destinationKey, irr.getUploadId()));
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
