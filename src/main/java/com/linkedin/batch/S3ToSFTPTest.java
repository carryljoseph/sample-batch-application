package com.linkedin.batch;

import java.io.File;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;

import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.repeat.RepeatStatus;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.S3Object;

import net.schmizz.sshj.SSHClient;
import net.schmizz.sshj.sftp.SFTPClient;
import net.schmizz.sshj.xfer.FileSystemFile;

public class S3ToSFTPTest implements Tasklet {
	
	private AWSCredentials credentials = new BasicAWSCredentials(
	          "", 
	          ""
	        );


	@Override
	public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) throws Exception {
    	AmazonS3 s3Client = AmazonS3ClientBuilder
		          .standard()
		          .withCredentials(new AWSStaticCredentialsProvider(credentials))
		          .withRegion(Regions.US_EAST_2)
		          .build();

        // s3 client
        if (s3Client == null) {
        	throw new Exception("S3 Client not found");
        }

        String bucketName = "";

        // s3 bucket - make sure it exist
        if (!s3Client.doesBucketExistV2(bucketName)) {
        	throw new Exception("Bucket does not exist in s3");
        }

        String fileName = "output_file/output.txt";
        File localFile = null;

        try {
            localFile = File.createTempFile("output", ".txt");

            // get S3Object
            S3Object s3Object = s3Client.getObject(bucketName, fileName);

            // get stream from S3Object
            InputStream inputStream = s3Object.getObjectContent();

            // write S3Object stream into a temp file
            Files.copy(inputStream, localFile.toPath(), StandardCopyOption.REPLACE_EXISTING);
        } catch (Exception e) {
        }

        if(localFile == null) {
        	throw new Exception("File is null in local");
        }
        RepeatStatus status = null;
        if(saveFilesToSFTP(localFile)) {
        	status = RepeatStatus.FINISHED;
        }else {
        	throw new Exception("Cant sent the file to Server");
        }
        return status;
    }


    public boolean saveFilesToSFTP(File... files) {
        final String sftpHostname = "";
        final String sftpUsername = "";
        final String sftpPassword = "";

        String remoteFolderPath = "files/output.txt";

        try {
            SSHClient ssh = new SSHClient();
            ssh.addHostKeyVerifier((hostname1, port, key) -> true);

            ssh.connect(sftpHostname);

            try {
                ssh.authPassword(sftpUsername, sftpPassword);

                try (SFTPClient sftp = ssh.newSFTPClient()) {
                    for(File file : files) {
                        sftp.put(new FileSystemFile(file), remoteFolderPath);
                    }
                } catch (Exception e) {
                	e.printStackTrace();
                    return false;
                }
            } finally {
            	ssh.disconnect();
            }
        } catch (Exception e) {
            return false;
        }

        return true;
    }
	
}

