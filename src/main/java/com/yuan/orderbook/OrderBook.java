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
	private Long targetSize;
	private Long topQueueSize;
	private Map<String, TradeOrder> topOrdersMap; //map of order-id to order of orders that will be executed with a trade of target-size
	private Map<String, TradeOrder> bottomOrdersMap; //map of order-id to order of orders that will not be executed with a trade of target-size
	private PriorityQueue<TradeOrder> topOrderQueue; //priority heap of orders that will be executed with a trade of target-size (ASK - highest price at top; BID - lowest price at top)
	private PriorityQueue<TradeOrder> bottomOrderQueue; //priority heap of orders that will not be executed with a trade of target-size (ASK - lowest price at top; BID - highest price at top)
	private BigDecimal cachedTradeValue; //if the trade value at target-size need to be recomputed, null; otherwise this stores the trade value at target0-size
	
	/**
	 * Given a side (i.e. BID or ASK), construct an instance of the TradeBook
	 * for Bid or Ask respectively.
	 * 
	 * @param side
	 * @param targetSize
	 */
	public OrderBook(Side side, Long targetSize){
		this.side = side;
		this.targetSize = targetSize;
		this.topQueueSize = (long) 0;
		this.topOrdersMap = new HashMap<String, TradeOrder>();
		this.bottomOrdersMap = new HashMap<String, TradeOrder>();
		if(this.side==Side.ASK){
			this.topOrderQueue = new PriorityQueue<TradeOrder>(11, Collections.reverseOrder()); //highest non-executed price on top
			this.bottomOrderQueue = new PriorityQueue<TradeOrder>(); //lowest non-executed price on top
		}else{
			this.topOrderQueue = new PriorityQueue<TradeOrder>(); //lowest executed price on top
			this.bottomOrderQueue = new PriorityQueue<TradeOrder>(11, Collections.reverseOrder()); //highest non-executed price on top
			
		}
		this.cachedTradeValue= BigDecimal.ZERO;
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
		
		if(this.checkBelongToTopQueue(tradeOrder)){
			this.topOrdersMap.put(tradeID, tradeOrder);
			this.topOrderQueue.add(tradeOrder);
			this.topQueueSize += orderSize;
			this.cachedTradeValue = this.cachedTradeValue.add(price.multiply(new BigDecimal(orderSize)));
		
			while(this.topQueueSize > this.targetSize){
				//remove from top-heap
				TradeOrder currOrder = this.topOrderQueue.remove();
				this.topOrdersMap.remove(currOrder.getOrderID());
				
				//move to bottom-heap
				Long quantityToShift = Math.min(currOrder.getOrderSize(), this.topQueueSize-this.targetSize);
				Long quantityRemaining = currOrder.getOrderSize() - quantityToShift;
				
				Long existingQuantity = Long.valueOf(0);
				if(this.bottomOrdersMap.containsKey(currOrder.getOrderID())){
					existingQuantity = this.bottomOrdersMap.get(currOrder.getOrderID()).getOrderSize();
					this.bottomOrderQueue.remove(this.bottomOrdersMap.get(currOrder.getOrderID()));
				}
				
				TradeOrder shiftedOrder = new TradeOrder(currOrder.getOrderID(), currOrder.getTimeStampMilliSec(), 
						currOrder.getSide(), currOrder.getPrice(), existingQuantity+quantityToShift);
				this.bottomOrderQueue.add(shiftedOrder);
				this.bottomOrdersMap.put(shiftedOrder.getOrderID(), shiftedOrder);
				
				this.topQueueSize -= quantityToShift;
//				System.out.println("shifted order price" + shiftedOrder.getPrice().toString());
				
				this.cachedTradeValue = this.cachedTradeValue
						.subtract(shiftedOrder.getPrice().multiply(new BigDecimal(quantityToShift)));
				
				if(quantityRemaining > 0){
					TradeOrder remainingOrder = new TradeOrder(currOrder.getOrderID(), currOrder.getTimeStampMilliSec(), 
							currOrder.getSide(), currOrder.getPrice(), quantityRemaining);
					this.topOrderQueue.add(remainingOrder);
					this.topOrdersMap.put(remainingOrder.getOrderID(), remainingOrder);
				}
			}
			
		}else{
			this.bottomOrderQueue.add(tradeOrder);
			this.bottomOrdersMap.put(tradeID, tradeOrder);
		}
//		if(this.side==Side.BID){
//			System.out.println("BID top queue size" + this.topQueueSize.toString());
//			System.out.println("value" + this.cachedTradeValue);
//			System.out.println(this.topOrdersMap.toString());
//			System.out.println(this.bottomOrdersMap.toString());
//		}else{
//			System.out.println("ASK top queue size" + this.topQueueSize.toString());
//			System.out.println("value" + this.cachedTradeValue);
//			System.out.println(this.topOrdersMap.toString());
//			System.out.println(this.bottomOrdersMap.toString());
//		}
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
		
		if(this.bottomOrdersMap.containsKey(tradeID) || this.topOrdersMap.containsKey(tradeID)){
			
			Long quantityReduced = Long.valueOf(0);
			if(this.bottomOrdersMap.containsKey(tradeID)){
				TradeOrder tradeOrder = this.bottomOrdersMap.get(tradeID);
				//remove the order from the queue, we will add back
				//the reduced order if it still has positive size
				this.bottomOrderQueue.remove(tradeOrder);
				
				//reduce the order
				quantityReduced = Math.min(tradeOrder.getOrderSize(), reductionSize);
				tradeOrder.reduce(quantityReduced);
				
				if(tradeOrder.getOrderSize() == 0){
					//if the size of the trade order is reduced to zero, remove it from the map
					this.bottomOrdersMap.remove(tradeID);
				}else{
					//if the reduced order still has positive size, add it back to the queue
					this.bottomOrderQueue.add(tradeOrder);
				}
			}
			
			Long quantityStillToBeReduced = reductionSize - quantityReduced;
			if(quantityStillToBeReduced > 0){
				if(this.topOrdersMap.containsKey(tradeID)){
					TradeOrder tradeOrder = this.topOrdersMap.get(tradeID);
					//remove the order from the queue, we will add back
					//the reduced order if it still has positive size
					this.topOrderQueue.remove(tradeOrder);
					
					//reduce the order
					tradeOrder.reduce(quantityStillToBeReduced);
					this.topQueueSize -= quantityStillToBeReduced;
					this.cachedTradeValue = this.cachedTradeValue
							.subtract(tradeOrder.getPrice().multiply(new BigDecimal(quantityStillToBeReduced)));
					
					if(tradeOrder.getOrderSize() == 0){
						//if the size of the trade order is reduced to zero, remove it from the map
						this.topOrdersMap.remove(tradeID);
					}else{
						//if the reduced order still has positive size, add it back to the queue
						this.topOrderQueue.add(tradeOrder);
					}
					
					while(this.topQueueSize < this.targetSize && this.bottomOrderQueue.size()>0){
						//remove from bottom-heap
						TradeOrder currOrder = this.bottomOrderQueue.remove();
						this.bottomOrdersMap.remove(currOrder.getOrderID());
						
						//move to top
						Long quantityToShift = Math.min(currOrder.getOrderSize(), this.targetSize-this.topQueueSize);
						Long quantityRemaining = currOrder.getOrderSize() - quantityToShift;
						
						Long existingQuantity = Long.valueOf(0);
						if(this.topOrdersMap.containsKey(currOrder.getOrderID())){
							existingQuantity = this.topOrdersMap.get(currOrder.getOrderID()).getOrderSize();
							this.topOrderQueue.remove(this.topOrdersMap.get(currOrder.getOrderID()));
						}
						TradeOrder shiftedOrder = new TradeOrder(currOrder.getOrderID(), currOrder.getTimeStampMilliSec(), 
								currOrder.getSide(), currOrder.getPrice(), existingQuantity + quantityToShift);
						this.topOrderQueue.add(shiftedOrder);
						this.topOrdersMap.put(shiftedOrder.getOrderID(), shiftedOrder);
						this.cachedTradeValue = this.cachedTradeValue
								.add(shiftedOrder.getPrice().multiply(new BigDecimal(quantityToShift)));
						this.topQueueSize += quantityToShift;
						
						if(quantityRemaining > 0){
							TradeOrder remainingOrder = new TradeOrder(currOrder.getOrderID(), currOrder.getTimeStampMilliSec(), 
									currOrder.getSide(), currOrder.getPrice(), quantityRemaining);
							this.bottomOrderQueue.add(remainingOrder);
							this.bottomOrdersMap.put(remainingOrder.getOrderID(), remainingOrder);
						}
					}
				}else{
					System.out.println("something weird happend");
					throw new TradeOrderException("Reduction order larger than existing trade orders.");
				}
				
			}
			
//			if(this.side==Side.BID){
//				System.out.println("BID top queue size" + this.topQueueSize.toString());
//				System.out.println("value" + this.cachedTradeValue);
//				System.out.println(this.topOrdersMap.toString());
//				System.out.println(this.bottomOrdersMap.toString());
//			}else{
//				System.out.println("ASK top queue size" + this.topQueueSize.toString());
//				System.out.println("value" + this.cachedTradeValue);
//				System.out.println(this.topOrdersMap.toString());
//				System.out.println(this.bottomOrdersMap.toString());
//			}
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
	public BigDecimal computeValue(){
		if(this.topQueueSize.equals(this.targetSize)){
			return this.cachedTradeValue;
		}else{
			return new BigDecimal(-1); //indicate not enough orders in the book to execute the trade
		}

	}
	
	/**
	 * Return a boolean indicating whether a trade-id matches a trade in the trade-book
	 * 
	 * @param tradeID
	 * @return
	 */
	public boolean contains(String tradeID){
		return this.topOrdersMap.containsKey(tradeID) 
				|| this.bottomOrdersMap.containsKey(tradeID);
	}
	
	private Boolean checkBelongToTopQueue(TradeOrder tradeOrder){
		if(this.topQueueSize < this.targetSize){
			return true;
		}else{
			TradeOrder worstExecutedOrder = this.topOrderQueue.peek();
			if(this.side==Side.ASK){
				return tradeOrder.getPrice().compareTo(worstExecutedOrder.getPrice()) < 0;
			}else{
				return tradeOrder.getPrice().compareTo(worstExecutedOrder.getPrice()) > 0;
			}
		}
	}
	

}
