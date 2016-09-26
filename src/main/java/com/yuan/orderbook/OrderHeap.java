package com.yuan.orderbook;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;

/**
 * A priority heap (either min or max) for trade orders that allows the orders
 * to be retrieved by orderID. New orders can be inserted in O(log(n)) time
 * and the top order can be removed in O(log(n)) time.
 * The heap also allows O(1) time access to the total value and total order size 
 * of the orders in the heap. 
 * 
 * @author yuan
 *
 */
public class OrderHeap {
	
	private PriorityQueue<TradeOrder> orderHeap; //heap of orders
	private Map<String, TradeOrder> orderMap; //map of orders: orderID -> order
	private BigDecimal totalValue; // sum of the order-size * price of the orders in the heap
	private Long totalQuantity; //sum of the order sizes of the orders in the heap
	
	public OrderHeap(Comparator<Object> comparator){
		this.orderHeap = new PriorityQueue<TradeOrder>(11, comparator);
		this.orderMap = new HashMap<String,TradeOrder>();
		this.totalValue = BigDecimal.ZERO;
		this.totalQuantity = Long.valueOf(0);
	}
	
	/**
	 * Return the dollar value of all of the orders in the heap
	 * 
	 * @return
	 */
	public BigDecimal getTotalValue(){
		return this.totalValue;
	}
	
	/**
	 * Return the sum of the order sizes of all of the orders in the heap.
	 * 
	 * @return
	 */
	public Long getTotalQuantity(){
		return this.totalQuantity;
	}
	
	/**
	 * Return boolean indicating whether an order exists in the heap for the specified orderID
	 * 
	 * @param orderID
	 * @return
	 */
	public boolean containsKey(String orderID){
		return this.orderMap.containsKey(orderID);
	}
	
	/**
	 * Return a long indicating the order size in the heap for the specified orderID
	 * 
	 * @param orderID
	 * @return
	 */
	public Long getOrderSizeByID(String orderID){
		if(this.orderMap.containsKey(orderID)){
			return this.orderMap.get(orderID).getOrderSize();
		}else{
			return Long.valueOf(0);
		}
	}
	
	/**
	 * Return the top order without removing it from the heap.
	 * 
	 * @return
	 */
	public TradeOrder peek(){
		return this.orderHeap.peek();
	}
	
	/**
	 * Add an order to the order heap. As we allow a single order to be split between the top heap
	 * and bottom heap, the order to be added may already exist in the heap. If the order already
	 * exist, we increment its quantity by the orderSize of the order to be added. Otherwise,
	 * we insert a new order into the heap.
	 * 
	 * @param tradeOrder
	 * @throws TradeOrderException
	 */
	public void addTradeOrder(TradeOrder tradeOrder) throws TradeOrderException{
		String orderID = tradeOrder.getOrderID();
		if(this.orderMap.containsKey(orderID)){ //this is adding to an existing order in the heap
			TradeOrder existingOrder = this.orderMap.get(orderID);
			Long existingQuantity = existingOrder.getOrderSize();
			//build updated order with combined order size
			TradeOrder updatedOrder = new TradeOrder(orderID, tradeOrder.getTimeStampMilliSec(), tradeOrder.getSide(), 
					tradeOrder.getPrice(), existingQuantity+tradeOrder.getOrderSize());
			//remove existing order
			this.orderHeap.remove(existingOrder);
			this.orderMap.remove(orderID);
			//insert updated order
			this.orderHeap.add(updatedOrder);
			this.orderMap.put(orderID, updatedOrder);
		}else{//this is a new order for the heap
			this.orderHeap.add(tradeOrder);
			this.orderMap.put(orderID, tradeOrder);
		}
		//update totalQuantity and totalValue
		this.totalQuantity += tradeOrder.getOrderSize();
		this.totalValue = this.totalValue.add(tradeOrder.getPrice().multiply(new BigDecimal(tradeOrder.getOrderSize())));
	}
	
	/**
	 * Add a list of orders to the heap. 
	 * 
	 * @param tradeOrders
	 * @throws TradeOrderException
	 */
	public void addTradeOrders(List<TradeOrder> tradeOrders) throws TradeOrderException{
		for(TradeOrder tradeOrder : tradeOrders){
			this.addTradeOrder(tradeOrder);
		}
	}
	
	/**
	 * Remove a single order identified by the orderID and return it
	 * 
	 * @param orderID
	 * @return
	 */
	public TradeOrder removeOrderByID(String orderID){
		TradeOrder tradeOrder = this.orderMap.get(orderID);
		this.orderHeap.remove(tradeOrder);
		this.orderMap.remove(orderID);
		this.totalQuantity -= tradeOrder.getOrderSize();
		this.totalValue = this.totalValue
				.subtract(tradeOrder.getPrice().multiply(new BigDecimal(tradeOrder.getOrderSize())));
		return tradeOrder;
	}
	
	/**
	 * If the heap's totalQuantity is greater than targetSize, reduce the heap to a targetSize
	 * and return a list of orders removed from the heap in order to reduce it to the targetSize.
	 * Otherwise, do not change the heap and return an empty list.
	 * 
	 * @param targetSize
	 * @return
	 * @throws TradeOrderException
	 */
	public List<TradeOrder> reduceHeapToTargetSize(Long targetSize) throws TradeOrderException{
		Long reductionSize = Math.max(this.totalQuantity-targetSize, 0);
		return this.reduceHeapBySize(reductionSize);
	}
	
	/**
	 * Reduce the heap by a specified quantity (reductionSize) and return the list of
	 * orders removed from the heap. If the reductionSize is greater than the totalQuantity
	 * of the heap, then throw an IllegalArgumentException.
	 * 
	 * @param reductionSize
	 * @return
	 * @throws TradeOrderException
	 */
	public List<TradeOrder> reduceHeapBySize(Long reductionSize) throws TradeOrderException{
		if(reductionSize <= this.getTotalQuantity()){
			List<TradeOrder> tradeOrdersPopped = new ArrayList<TradeOrder>(); //accumulate the trade-orders that have been poppoed so far
			Long quantityReduced = Long.valueOf(0); //accumulate the quantity reduced so far
			while(quantityReduced < reductionSize){
				//remove the top order
				TradeOrder currOrder = this.orderHeap.remove();
				this.orderMap.remove(currOrder.getOrderID());
				
				//compute the quantity reduction, reduce the totalQuantity and totalValue and add to the list of popped trades
				Long quantityReduction = Math.min(currOrder.getOrderSize(), reductionSize - quantityReduced);
				this.totalQuantity -= quantityReduction;
				this.totalValue = this.totalValue.subtract(currOrder.getPrice().multiply(new BigDecimal(quantityReduction)));
				quantityReduced += quantityReduction;
				
				TradeOrder tradeOrderPopped = new TradeOrder(currOrder.getOrderID(), currOrder.getTimeStampMilliSec(),
						currOrder.getSide(), currOrder.getPrice(), quantityReduction);
				tradeOrdersPopped.add(tradeOrderPopped);
				
				//if a portion of the order remains, add it back to the heap
				Long quantityRemaining = currOrder.getOrderSize() - quantityReduction;
				if (quantityRemaining > 0){
					TradeOrder remainingOrder = new TradeOrder(currOrder.getOrderID(), currOrder.getTimeStampMilliSec(), 
							currOrder.getSide(), currOrder.getPrice(), quantityRemaining);
					this.orderHeap.add(remainingOrder);
					this.orderMap.put(remainingOrder.getOrderID(), remainingOrder);
				}
			}
			return tradeOrdersPopped;
		}else{
			throw new IllegalArgumentException("the reduction size is greater than the size of the heap.");
		}
	}
	
	
	@Override
	public String toString(){
		return "{total_quantity: " + Long.toString(this.totalQuantity) + ",\n" +
				"total_value: " + this.totalValue.toString() + ",\n" +
				"orders (unordered): " + this.orderMap.toString() + "}";
	}
}
