package com.yuan.orderbook;

import java.math.BigDecimal;

public class TradeOrder implements Comparable<TradeOrder>{
	
	public enum Side {BID, ASK}
	
	private String orderID;
	private Long timeStampMilliSecs; //timeStamp in milliseconds from mid-night
	private Side side;
	private BigDecimal price; //price per share in cents to avoid floating point precision issues
	private Long orderSize; 
	
	/**
	 * Construct a new trade order.
	 * 
	 * @param orderID
	 * @param timeStampMilliSecs
	 * @param side
	 * @param price
	 * @param orderSize
	 * @throws TradeOrderException 
	 */
	public TradeOrder(String orderID, Long timeStampMilliSecs, Side side, BigDecimal price, Long orderSize) 
			throws TradeOrderException{
		
		validateNewOrderArgs(orderID, timeStampMilliSecs, price, orderSize);
		
		this.orderID = orderID;
		this.timeStampMilliSecs = timeStampMilliSecs;
		this.side = side;
		this.price = price;
		this.orderSize = orderSize;
	}
	
	/**
	 * Reduce the quantity of the order
	 * 
	 * @param quantity
	 * @throws TradeOrderException
	 */
	public void reduce(Long quantity) 
			throws TradeOrderException{
		validateOrderReductionArgs(this.orderSize, quantity);
		this.orderSize -= quantity;
	}
	
	/**
	 * Return the trade order id
	 * 
	 * @return
	 */
	public String getOrderID(){
		return this.orderID;
	}
	
	/**
	 * Return the time stamp in millisec since midnight
	 * 
	 * @return
	 */
	public Long getTimeStampMilliSec(){
		return this.timeStampMilliSecs;
	}
	
	public Side getSide(){
		return this.side;
	}
	
	/**
	 * Return the size of the order
	 * 
	 * @return
	 */
	public Long getOrderSize(){
		return this.orderSize;
	}
	
	
	/**
	 * Return the price of the order
	 * 
	 * @return
	 */
	public BigDecimal getPrice(){
		return this.price;
	}
	
	/**
	 * Compare orders based on their price.
	 * 
	 */
	@Override
	public int compareTo(TradeOrder other) {
		if(other == null){
			throw new NullPointerException();
		}else{
			TradeOrder orderTwo = (TradeOrder) other;
			return this.getPrice().compareTo(orderTwo.getPrice());
		}
	}
	
	
	//==============================//
	// Private validation functions	//
	//==============================//
	/**
	 * Validate the arguments of a new order constructor call
	 * 
	 * @param orderID
	 * @param timeStampMilliSecs
	 * @param price
	 * @param orderSize
	 * @throws TradeOrderException
	 */
	private void validateNewOrderArgs(String orderID,
			Long timeStampMilliSecs, BigDecimal price, Long orderSize) 
					throws TradeOrderException {
		
		if(orderID == null || orderID.length()==0){ //check the order id is a non-empty string
			throw new TradeOrderException("Order ID is invalid.");
		}
		if(timeStampMilliSecs == null || timeStampMilliSecs < 0){ //check the time-stamp is >= 0
			throw new TradeOrderException("Time Stamp is invalid");
		}
		if(price == null || price.compareTo(BigDecimal.ZERO)<0){ //check the price is >= 0
			throw new TradeOrderException("Price is invalid");
		}
		if(orderSize == null || orderSize < 0){ //check the order size is >= 0
			throw new TradeOrderException("Order size is invalid");
		}
	}
	
	/**
	 * validate the arguments of a order reduction method call.
	 * The quantity of the reduction should be between 0 and 
	 * the size of the order.
	 * 
	 * @param orderSize
	 * @param reductionSize
	 * @throws TradeOrderException
	 */
	private void validateOrderReductionArgs(Long orderSize, Long reductionSize) 
			throws TradeOrderException{
		if(reductionSize == null 
				|| reductionSize < 0 
				|| reductionSize > orderSize){
			throw new TradeOrderException("Reduction size is invalid");
		}
	}
	
	@Override
	public String toString(){
		return "{Order ID: " + this.orderID + " Quantity: " + this.orderSize + " Price " + this.price +"}";
	}
}
