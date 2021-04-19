package com.linkedin.batch;

import java.util.List;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.StepExecutionListener;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.job.builder.FlowBuilder;
import org.springframework.batch.core.job.flow.Flow;
import org.springframework.batch.core.job.flow.JobExecutionDecider;
import org.springframework.batch.core.job.flow.support.SimpleFlow;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.core.task.SimpleAsyncTaskExecutor;

@SpringBootApplication
@EnableBatchProcessing
public class LinkedinBatchApplicationSample {
	
	@Autowired
	public JobBuilderFactory jobBuilderFactory;
	
	@Autowired
	public StepBuilderFactory stepBuilderFactory;
	
	@Bean
	public Step chunkBasedStep() {
		return this.stepBuilderFactory.get("chunkBasedStep")
				.<String,String>chunk(3)
				.reader(itemReader())
				.writer(new ItemWriter<String>() {

					@Override
					public void write(List<? extends String> items) throws Exception {
						System.out.println(String.format("Received list of size: %s", items.size()));
						items.forEach(System.out::println);
					}
					
				}).build();
	}
	
	
	@Bean
	public ItemReader<String> itemReader() {
		return new SimpleItemReader();
	}

	@Bean
	public Job job() {
		return this.jobBuilderFactory.get("job")
				.start(chunkBasedStep())
				.build();
	}
	
	@Bean
	public JobExecutionDecider decider() {
		return new DeliveryDecider();
	}
	
	@Bean
	public JobExecutionDecider refundDecider() {
		return new RefundDecider();
	}
	
	@Bean
	public Step nestedBillingJobStep() {
		return this.stepBuilderFactory.get("nestedBillingJobStep").job(billingJob()).build();
	}
	
	@Bean
	public Job billingJob() {
		return this.jobBuilderFactory.get("billingJob").start(sendInvoiceStep()).build();
	}
	
	@Bean
	public Flow billingFlow() {
		return new FlowBuilder<SimpleFlow>("billingFlow").start(sendInvoiceStep()).build();
	}
	
	@Bean
	public Step sendInvoiceStep() {
		return this.stepBuilderFactory.get("invoiceStep").tasklet(new Tasklet() {
			
			@Override
			public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) throws Exception {
				System.out.println("Invoice is sent to customer");
				return RepeatStatus.FINISHED;
			}
		}).build();
	}

	@Bean
	public Flow deliveryFlow() {
		return new FlowBuilder<SimpleFlow>("deliveryFlow").start(driveToAddressStep())
				.on("FAILED")
				.to(storePackageStep())
				.from(driveToAddressStep())
					.on("*").to(decider())
						.on("PRESENT")
						.to(givePackageToCustomerStep())
						.next(refundDecider())
						.on("CORRECT")
						.to(thankTheCustomerStep())
						.from(refundDecider())
						.on("INCORRECT").to(refundStep())
						.from(decider())
						.on("NOT_PRESENT").to(leaveAtDoorStep()).build();
	}
	
	@Bean
	public StepExecutionListener selectFlowerListener() {
		return new FlowersSelectionStepExecutionListener();
	}
	
	@Bean
	public Step leaveAtDoorStep() {
		return this.stepBuilderFactory.get("leaveAtDoorStep").tasklet(new Tasklet() {
			
			@Override
			public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) throws Exception {
				System.out.println("Leaving the package at door.");
				return RepeatStatus.FINISHED;
			}
		}).build();
	}
	
	@Bean
	public Step thankTheCustomerStep() {
		return this.stepBuilderFactory.get("thankTheCustomerStep").tasklet(new Tasklet() {
			
			@Override
			public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) throws Exception {
				System.out.println("Thank you for shopping with us");
				return RepeatStatus.FINISHED;
			}
		}).build();
	}
	
	@Bean
	public Step refundStep() {
		return this.stepBuilderFactory.get("refundStep").tasklet(new Tasklet() {
			
			@Override
			public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) throws Exception {
				System.out.println("Refunding customer money");
				return RepeatStatus.FINISHED;
			}
		}).build();
	}
	
	@Bean
	public Step storePackageStep() {
		return this.stepBuilderFactory.get("storePackageStep").tasklet(new Tasklet() {
			
			@Override
			public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) throws Exception {
				System.out.println("Strong the package while the customer address is located");
				return RepeatStatus.FINISHED;
			}
		}).build();
	}
	
	@Bean
	public Step driveToAddressStep() {
		boolean GOT_LOST = false;
		return this.stepBuilderFactory.get("driveToAddressStep").tasklet(new Tasklet() {
			
			@Override
			public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) throws Exception {
				if(GOT_LOST) {
					throw new RuntimeException("Got lost driving to address");
				}
				System.out.println("Successfully arrived at the address.");
				return RepeatStatus.FINISHED;
			}
		}).build();
	}
	
	@Bean
	public Step givePackageToCustomerStep() {
		return this.stepBuilderFactory.get("givePackageToCustomerStep").tasklet(new Tasklet() {
			
			@Override
			public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) throws Exception {
				System.out.println("Given the package to the customer");
				return RepeatStatus.FINISHED;
			}
		}).build();
	}
	
	@Bean
	public Step selectFlowersStep() {
		return this.stepBuilderFactory.get("selectFlowersStep").tasklet(new Tasklet() {
			
			@Override
			public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) throws Exception {
				System.out.println("Given the package to the customer");
				return RepeatStatus.FINISHED;
			}
		}).listener(selectFlowerListener()).build();
	}
	
	@Bean
	public Step removeThornsStep() {
		return this.stepBuilderFactory.get("removeThornsStep").tasklet(new Tasklet() {
			
			@Override
			public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) throws Exception {
				System.out.println("Remove thorns from roses.");
				return RepeatStatus.FINISHED;
			}
		}).build();
	}
	
	@Bean
	public Step arrangeFlowersStep() {
		return this.stepBuilderFactory.get("arrangeFlowersStep").tasklet(new Tasklet() {
			
			@Override
			public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) throws Exception {
				System.out.println("Arranging flowers for order.");
				return RepeatStatus.FINISHED;
			}
		}).build();
	}
	
	@Bean
	public Job prepareFlowersJob() {
		 return this.jobBuilderFactory.get("prepareFlowersJob")
				 .start(selectFlowersStep())
				 	.on("TRIM_REQUIRED").to(removeThornsStep()).next(arrangeFlowersStep())
				 .from(selectFlowersStep())
					.on("NO_TRIM_REQUIRED").to(arrangeFlowersStep())
				.from(arrangeFlowersStep()).on("*").to(deliveryFlow())
				 .end()
				 .build();
				 
	}
	
	@Bean
	public Job deliverPackageJob() {
		return this.jobBuilderFactory.get("deliveryPackageJob")
				.start(packageItemStep())
//				.next(driveToAddressStep())
//				.on("FAILED")
////				.stop()
////				.fail()
//				.to(storePackageStep())
//				.from(driveToAddressStep())
//					.on("*").to(decider())
//						.on("PRESENT")
//						.to(givePackageToCustomerStep())
//						.next(refundDecider())
//						.on("CORRECT")
//						.to(thankTheCustomerStep())
//						.from(refundDecider())
//						.on("INCORRECT").to(refundStep())
//						.from(decider())
//						.on("NOT_PRESENT").to(leaveAtDoorStep())
				
				.split(new SimpleAsyncTaskExecutor())
				.add(deliveryFlow(),billingFlow())
//				.on("*").to(deliveryFlow())
//				.next(nestedBillingJobStep())
				.end().build();
				
				   
	}

	@Bean
	public Step packageItemStep() {
		return this.stepBuilderFactory.get("packageItemStep").tasklet(new Tasklet() {
			
			@Override
			public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) throws Exception {
				String item = chunkContext.getStepContext().getJobParameters().get("item").toString();
				String date = chunkContext.getStepContext().getJobParameters().get("run.date").toString();
				System.out.println(String.format("The %s has been packaged on %s", item,date));
				return RepeatStatus.FINISHED;
			}
		}).build();
	}

	public static void main(String[] args) {
		SpringApplication.run(LinkedinBatchApplication.class, args);
	}

}
