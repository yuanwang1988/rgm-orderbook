package com.yuan.orderbook;

import java.math.BigDecimal;
import java.util.Collections;
import java.util.List;
import com.yuan.orderbook.TradeOrder.Side;

/**
 * TradeBook represents the trade-book (ie. a bid-book or an ask-book).
 * 
 * @author yuan
 *
 */
public class OrderBook {
	
	private Side side; //{BID or ASK}
	private Long targetSize; //size of targeted trade
	private OrderHeap topHeap; //top heap keeps track of orders that would be executed against a trade of target-size
	private OrderHeap bottomHeap; //bottom heap keeps track of orders that would not be executed against a trade of target-size
	
	/**
	 * Given a side (i.e. BID or ASK), construct an instance of the TradeBook
	 * for Bid or Ask respectively.
	 * 
	 * @param side - side of the order book (i.e. ASK or BID)
	 * @param targetSize - size of target trade (in number of shares)
	 */
	public OrderBook(Side side, Long targetSize){
		this.side = side;
		this.targetSize = targetSize;
		if(this.side==Side.ASK){
			//for ask-orders, the lowest priced orders are executed first
			//the top of the top-heap is the highest priced ask-order that would be executed against a trade of target size
			//the top of the bottom-heap is the lowest priced ask-order that would not be executed against a trade of target size
			this.topHeap = new OrderHeap(Collections.reverseOrder());
			this.bottomHeap = new OrderHeap(null);
		}else{
			//for bid-orders, the highest priced orders are executed first
			//the top of the top-heap is the lowest priced bid-order that would be executed against a trade of target size
			//the top of the bottom-heap is the highest priced bid-order that would not be executed against a trade of target size
			this.topHeap = new OrderHeap(null); 
			this.bottomHeap = new OrderHeap(Collections.reverseOrder());  
		}
	}
	
	/**
	 * Given a new order (Bid or Ask), update the trade-book.
	 * 
	 * @param orderID
	 * @param timeStampMilliSecs
	 * @param side
	 * @param price
	 * @param orderSize
	 * @throws TradeOrderException - if the new add order does not conform to expected schema
	 */
	public void processNewOrder(String orderID, Long timeStampMilliSecs, Side side, BigDecimal price, Long orderSize) 
			throws TradeOrderException{
		
		//if an exception is thrown in constructing a new order, this method is terminated before any 
		//change is made to the underlying data so the invariant of the topHeap and bottomHeap is not broken.
		TradeOrder tradeOrder = new TradeOrder(orderID, timeStampMilliSecs, side, price, orderSize);
		
		if(this.checkBelongToTopHeap(tradeOrder)){
			this.topHeap.addTradeOrder(tradeOrder);
			//reduce the top-heap to target-size and shift the excess orders to bottom heap
			List<TradeOrder> topHeapOrdersPopped = this.topHeap.reduceHeapToTargetSize(this.targetSize);
			this.bottomHeap.addTradeOrders(topHeapOrdersPopped);
		}else{
			this.bottomHeap.addTradeOrder(tradeOrder);
		}
	}
		
	/**
	 * Given a reduce order, update the trade-book
	 * 
	 * @param orderID
	 * @param timeStampMilliScs
	 * @param reductionSize
	 * @throws TradeOrderException - if the reduce order does not conform to expected schema 
	 * or exceed existing order in size
	 */
	public void processReduceOrder(String orderID, Long timeStampMilliScs, Long reductionSize) 
			throws TradeOrderException{
		
		//check that the reduce order is valid (i.e. the reductionSize is less than or equal to 
		//the corresponding existing order in the orderbook). If the reduce order is not valid,
		//throw an exception rather than proceeding to avoid breaking the invariant of the 
		//topHeap and bottomHeap.
		if(this.bottomHeap.getOrderSizeByID(orderID) + this.topHeap.getOrderSizeByID(orderID) >= reductionSize){
			
			Long quantityReduced = Long.valueOf(0);
			//if the order is in the bottom heap, remove it from the bottom heap first before considering the top heap
			if(this.bottomHeap.containsKey(orderID)){
				TradeOrder bottomOrder = this.bottomHeap.removeOrderByID(orderID);
				quantityReduced = Math.min(bottomOrder.getOrderSize(), reductionSize);
				bottomOrder.reduce(quantityReduced);
				//if the order still has positive size, add it back to the bottom heap
				if(bottomOrder.getOrderSize()>0){
					this.bottomHeap.addTradeOrder(bottomOrder);
				}
			}
			//if the order reduction is greater than the order size in the bottom heap, remove from the top heap as well.
			Long quantityStillToReduce = reductionSize - quantityReduced;
			if(quantityStillToReduce > 0){
				TradeOrder topOrder = this.topHeap.removeOrderByID(orderID);
				topOrder.reduce(quantityStillToReduce);
				//if the order still has positive size, add it back to the top heap
				if(topOrder.getOrderSize()>0){
					this.topHeap.addTradeOrder(topOrder);
				}
			}
			//move orders from the bottom heap to the top heap until the top heap is at target-size or the bottom heap is empty
			Long quantityToShift = Math.min(this.targetSize-this.topHeap.getTotalQuantity(), this.bottomHeap.getTotalQuantity());
			List<TradeOrder> ordersShifted = this.bottomHeap.reduceHeapBySize(quantityToShift);
			this.topHeap.addTradeOrders(ordersShifted);
			
		}else{
			throw new TradeOrderException("Reduction order larger than existing trade orders.");
		}
	}
	
	/**
	 * If the order book contains sufficient orders to execute the target trade, 
	 * return the value to execute a trade of the target-size. 
	 * Otherwise, return -1.
	 * 
	 * @param targetSize - size of target trade
	 * @return
	 */
	public BigDecimal computeValue(){
		if(this.topHeap.getTotalQuantity().equals(this.targetSize)){
			return this.topHeap.getTotalValue();
		}else{
			return new BigDecimal(-1); //indicate not enough orders in the book to execute the trade
		}

	}
	
	/**
	 * Return a boolean indicating whether a orderID matches a trade in the trade-book
	 * 
	 * @param orderID
	 * @return
	 */
	public boolean contains(String orderID){
		return this.topHeap.containsKey(orderID) 
				|| this.bottomHeap.containsKey(orderID);
	}
	
	
	/**
	 * Given a new trade-order, determine whether it should be inserted into the
	 * top heap (i.e. orders that would be executed for a trade of target-size).
	 * 
	 * @param tradeOrder
	 * @return
	 */
	private Boolean checkBelongToTopHeap(TradeOrder tradeOrder){
		if(this.topHeap.getTotalQuantity() < this.targetSize){
			return true;
		}else{
			TradeOrder worstExecutedOrder = this.topHeap.peek();
			if(this.side==Side.ASK){ // for ask orders, return true if the price is less than the top of the top-heap
				return tradeOrder.getPrice().compareTo(worstExecutedOrder.getPrice()) < 0;
			}else{ // for bid orders, return true if the price is greater than the top of the top-heap
				return tradeOrder.getPrice().compareTo(worstExecutedOrder.getPrice()) > 0;
			}
		}
	}
	
	@Override
	public String toString(){
		String side = (this.side == Side.ASK) ? "ASK" : "BID";
		
		return "{side: " + side + ",\n" +
				"target_size: " + Long.toString(this.targetSize) + ",\n" +
				"top_order_heap: " + this.topHeap.toString() + ",\n" + 
				"bottom_order_heap: " + this.bottomHeap.toString() + "}";
	}

}
