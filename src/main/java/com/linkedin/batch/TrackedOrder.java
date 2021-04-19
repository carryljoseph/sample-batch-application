package com.linkedin.batch;

import java.math.BigDecimal;
import java.util.Date;

import javax.validation.constraints.Pattern;

import org.springframework.beans.BeanUtils;

public class TrackedOrder extends Order {
	
	private Long orderId;

	private String firstName;

	private String lastName;

	@Pattern(regexp = ".*\\.gov")
	private String email;
	
	private BigDecimal cost;
	
	private String itemId;
	
	private String itemName;
	
	private Date shipDate;
	
	private String trackingNumber;
	
	private Boolean freeShipping;

	public Long getOrderId() {
		return orderId;
	}

	public void setOrderId(Long orderId) {
		this.orderId = orderId;
	}

	public String getFirstName() {
		return firstName;
	}

	public void setFirstName(String firstName) {
		this.firstName = firstName;
	}

	public String getLastName() {
		return lastName;
	}

	public void setLastName(String lastName) {
		this.lastName = lastName;
	}

	public String getEmail() {
		return email;
	}

	public void setEmail(String email) {
		this.email = email;
	}

	public BigDecimal getCost() {
		return cost;
	}

	public void setCost(BigDecimal cost) {
		this.cost = cost;
	}

	public String getItemId() {
		return itemId;
	}

	public void setItemId(String itemId) {
		this.itemId = itemId;
	}

	public String getItemName() {
		return itemName;
	}

	public void setItemName(String itemName) {
		this.itemName = itemName;
	}

	public Date getShipDate() {
		return shipDate;
	}

	public void setShipDate(Date shipDate) {
		this.shipDate = shipDate;
	}


	public TrackedOrder() {

	}

	public TrackedOrder(Order order) {
		BeanUtils.copyProperties(order, this);
	}

	public String getTrackingNumber() {
		return trackingNumber;
	}

	public void setTrackingNumber(String trackingNumber) {
		this.trackingNumber = trackingNumber;
	}

	public Boolean getFreeShipping() {
		return freeShipping;
	}

	public void setFreeShipping(Boolean freeShipping) {
		this.freeShipping = freeShipping;
	}


}
