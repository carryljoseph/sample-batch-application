package com.linkedin.batch;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import org.springframework.core.io.AbstractResource;
import org.springframework.core.io.WritableResource;

public class S3Resource extends AbstractResource implements WritableResource {

	   ByteArrayOutputStream resource = new ByteArrayOutputStream();

	    @Override
	    public String getDescription() {
	        return null;
	    }

	    @Override
	    public InputStream getInputStream() throws IOException {
	        return new ByteArrayInputStream(resource.toByteArray());
	    }

	    @Override
	    public OutputStream getOutputStream() throws IOException {
	        return resource;
	    }
	}