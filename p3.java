import java.io.File;
import java.io.IOException;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.CreateBucketRequest;
import com.amazonaws.services.s3.model.PutObjectRequest;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.Protocol;
import com.amazonaws.services.s3.S3ClientOptions;


public class p3 {

	private static String bucketName     = "my-new-bucket";
	private static String keyName        = "iat14.ogg";
	private static String uploadFileName = "/home/mdw/iat14.ogg";
	
	private static String accessKey = "40JV0D93WB9U33YNQ0L4";
	private static String secretKey = "8GjwnD4iocK3gUuOAbIj9G/9QFeXgIt+n9rJk06O";
	
	public static void main(String[] args) {
		ClientConfiguration clientConfig = new ClientConfiguration();
		clientConfig.setProtocol(Protocol.HTTP);

		//AmazonS3 s3client = new AmazonS3Client(new ProfileCredentialsProvider(), clientConfig);
		AWSCredentials credentials = new BasicAWSCredentials(accessKey, secretKey);
		AmazonS3 s3client = new AmazonS3Client(credentials); //

		// TODO: can I set an explicit port?
		s3client.setEndpoint("http://hybodus.eng.arb.redhat.com:90");

		s3client.setS3ClientOptions(new S3ClientOptions().withPathStyleAccess(true));
        	try {
			if(!(s3client.doesBucketExist(bucketName)))
            		{
            			s3client.createBucket(new CreateBucketRequest(bucketName));
            		}
            		System.out.println("Uploading a new object to S3 from a file\n");
            		File file = new File(uploadFileName);
            		s3client.putObject(new PutObjectRequest(bucketName, keyName, file));

         	} catch (AmazonServiceException ase) {
            		System.out.println("Caught an AmazonServiceException, which " +
            				   "means your request made it " +
                    			   "to Amazon S3, but was rejected with an error response" +
                    			   " for some reason.");
            		System.out.println("Error Message:    " + ase.getMessage());
            		System.out.println("HTTP Status Code: " + ase.getStatusCode());
            		System.out.println("AWS Error Code:   " + ase.getErrorCode());
            		System.out.println("Error Type:       " + ase.getErrorType());
            		System.out.println("Request ID:       " + ase.getRequestId());
        	} catch (AmazonClientException ace) {
            		System.out.println("Caught an AmazonClientException, which " +
            				   "means the client encountered " +
                    			   "an internal error while trying to " +
                    			   "communicate with S3, " +
                    			   "such as not being able to access the network.");
            		System.out.println("Error Message: " + ace.getMessage());
        }

	}

}
