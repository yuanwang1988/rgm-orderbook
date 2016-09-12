package com.yuan.orderbook;

public class TradeOrderException extends Exception {

	private static final long serialVersionUID = 7808877832680533257L;
	
	public TradeOrderException(String reason){
		super(reason);
	}

}
