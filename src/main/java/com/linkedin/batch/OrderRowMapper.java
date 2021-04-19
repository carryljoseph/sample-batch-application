package com.linkedin.batch;

import java.sql.ResultSet;
import java.sql.SQLException;

import org.springframework.jdbc.core.RowMapper;

public class OrderRowMapper implements RowMapper<TrackedOrder> {

	@Override
	public TrackedOrder mapRow(ResultSet rs, int rowNum) throws SQLException {
		TrackedOrder order = new TrackedOrder();
		order.setOrderId(rs.getLong("order_id"));
		order.setCost(rs.getBigDecimal("cost"));
		order.setEmail(rs.getString("email"));
		order.setFirstName(rs.getString("first_name"));
		order.setLastName(rs.getString("last_name"));
		order.setItemId(rs.getString("item_id"));
		order.setItemName(rs.getString("item_name"));
		order.setShipDate(rs.getDate("ship_date"));
		order.setTrackingNumber(rs.getString("tracking_number"));
		order.setFreeShipping(rs.getBoolean("free_shipping"));
		return order;
	}

}
