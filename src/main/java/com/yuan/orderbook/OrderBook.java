package com.yuan.orderbook;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;

import com.yuan.orderbook.TradeOrder.Side;

/**
 * TradeBook represents the trade-book (ie. a bid-book or an ask-book)
 * 
 * @author yuan
 *
 */
public class OrderBook {
	
	private Side side; //{BID or ASK}
	private Long totalSize; //total quantity (in shares) of orders in the book
	private Map<String, TradeOrder> ordersMap; //map of order-id to order
	private PriorityQueue<TradeOrder> orderQueue; //priority heap of orders (ASK - lowest price at top; BID - highest price at top)
	private BigDecimal thresholdPrice; //if the new order or reduce order meets a threshold, it could affect the trade-value at the target-size
	private BigDecimal cachedTradeValue; //if the trade value at target-size need to be recomputed, null; otherwise this stores the trade value at target0-size
	
	/**
	 * Given a side (i.e. BID or ASK), construct an instance of the TradeBook
	 * for Bid or Ask respectively.
	 * 
	 * @param side
	 */
	public OrderBook(Side side){
		this.side = side;
		this.totalSize = (long) 0;
		this.ordersMap = new HashMap<String, TradeOrder>();
		if(this.side==Side.ASK){
			this.orderQueue = new PriorityQueue<TradeOrder>();
		}else{
			this.orderQueue = new PriorityQueue<TradeOrder>(11, Collections.reverseOrder());
		}
		this.thresholdPrice=null;
		this.cachedTradeValue=null;
	}
	
	/**
	 * Given a new order (Bid or Ask), update the trade-book.
	 * 
	 * @param tradeID
	 * @param timeStampMilliSecs
	 * @param side
	 * @param price
	 * @param orderSize
	 * @throws TradeOrderException
	 */
	public void processNewOrder(String tradeID, Long timeStampMilliSecs, Side side, BigDecimal price, Long orderSize) 
			throws TradeOrderException{
		TradeOrder tradeOrder = new TradeOrder(tradeID, timeStampMilliSecs, side, price, orderSize);
		this.invalidateCachedTradeValueIfAppropriate(price);
		this.ordersMap.put(tradeID, tradeOrder);
		this.orderQueue.add(tradeOrder);
		this.totalSize += orderSize;	
	}
	
	/**
	 * Given a reduce order, update the trade-book
	 * 
	 * @param tradeID
	 * @param timeStampMilliScs
	 * @param reductionSize
	 * @throws TradeOrderException
	 */
	public void processReduceOrder(String tradeID, Long timeStampMilliScs, Long reductionSize) 
			throws TradeOrderException{
		if(ordersMap.containsKey(tradeID)){
			TradeOrder tradeOrder = ordersMap.get(tradeID);
			this.invalidateCachedTradeValueIfAppropriate(tradeOrder.getPrice());
			
			//remove the order from the queue, we will add back
			//the reduced order if it still has positive size
			this.orderQueue.remove(tradeOrder);
			
			//reduce the order
			tradeOrder.reduce(reductionSize);
			this.totalSize -= reductionSize;
			
			if(tradeOrder.getOrderSize() == 0){
				//if the size of the trade order is reduced to zero, remove it from the map
				this.ordersMap.remove(tradeID);
				this.orderQueue.remove(tradeOrder);
			}else{
				//if the reduced order still has positive size, add it back to the queue
				this.orderQueue.add(tradeOrder);
			}
			
		}else{
			throw new TradeOrderException("Unknown trade ID in the reduction order");
		}
	}
	
	/**
	 * Return the current value to trade the target-size.
	 * <p>
	 * NOTE: this method has a side-effect of updating the cachedTradeValue and thresholdPrice
	 * 
	 * @param targetSize
	 * @return
	 */
	public BigDecimal computeValue(Long targetSize){
		if (this.cachedTradeValue == null){
			TradeValueAndThresholdPriceCombo result = this.computeValueHelper(targetSize);
			this.cachedTradeValue = result.getTradeValue();
			this.thresholdPrice = result.getThresholdPrice();
		}
		return this.cachedTradeValue;

	}
	
	private TradeValueAndThresholdPriceCombo computeValueHelper(Long targetSize){
		if(this.totalSize >= targetSize){			
			BigDecimal currPrice = null; //this should be the threshold price after the while-loop
			BigDecimal valueTraded = BigDecimal.ZERO; //this should be the total trade-value after the while-loop
			List<TradeOrder> orderBuffer = new ArrayList<TradeOrder>(); //this stores all of the trades popped off during the while-loop
			
			Long quantityTraded = Long.valueOf(0);
			while(quantityTraded < targetSize){
				TradeOrder currOrder = this.orderQueue.remove(); //pop the best trade from the queue
				currPrice = currOrder.getPrice();
				orderBuffer.add(currOrder);
				Long currOrderQuantityTraded = Math.min(currOrder.getOrderSize(), targetSize - quantityTraded);
				quantityTraded += currOrderQuantityTraded;
				valueTraded = valueTraded.add(currOrder.getPrice().multiply(new BigDecimal(currOrderQuantityTraded)));
			}
			
			this.orderQueue.addAll(orderBuffer); //add back all of the popped off trades
			
			return new TradeValueAndThresholdPriceCombo(valueTraded, currPrice);
		}else{
			return new TradeValueAndThresholdPriceCombo(new BigDecimal(-1), null);
		}
	}
	
	/**
	 * Return a boolean indicating whether a trade-id matches a trade in the trade-book
	 * 
	 * @param tradeID
	 * @return
	 */
	public boolean contains(String tradeID){
		return this.ordersMap.containsKey(tradeID);
	}
	
	/**
	 * Check whether a new order or reduce order affects the trade-value to trade
	 * the target-size. If so, invalidate the cachedTradeValue by setting it to null.
	 * <p>
	 * NOTE: we chose to only have one method for new orders and reduce orders.
	 * We achieve slightly better performance with a separate method for new order
	 * and reduce order.
	 * 
	 * @param price
	 */
	private void invalidateCachedTradeValueIfAppropriate(BigDecimal price){
		if(this.side == Side.BID){//On BID side, trades with price >= threshold price could affect the trade value
			if(this.thresholdPrice == null || price.compareTo(this.thresholdPrice)>=0){
				this.cachedTradeValue = null; // invalidate the cached trade value
			}
		}else{//On ASK side, trades with price <= threshold price could affect the trade value
			if(this.thresholdPrice == null || price.compareTo(this.thresholdPrice)<=0){
				this.cachedTradeValue = null; // invalidate the cached trade value
			}
		}
		
	}
	
	/**
	 * Private class to hold a combination of trade-value and threshold-price
	 * 
	 * @author yuan
	 *
	 */
	private static class TradeValueAndThresholdPriceCombo{
		private BigDecimal tradeValue;
		private BigDecimal thresholdPrice;
		
		public TradeValueAndThresholdPriceCombo(BigDecimal tradeValue, BigDecimal thresholdPrice){
			this.tradeValue = tradeValue;
			this.thresholdPrice = thresholdPrice;
		}
		
		public BigDecimal getTradeValue(){
			return this.tradeValue;
		}
		
		public BigDecimal getThresholdPrice(){
			return this.thresholdPrice;
		}
	}
	

}
