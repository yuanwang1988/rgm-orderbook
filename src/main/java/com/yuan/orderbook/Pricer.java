package com.yuan.orderbook;

import java.math.BigDecimal;
import java.util.Objects;
import java.util.Scanner;

import com.yuan.orderbook.TradeOrder.Side;

/**
 * Pricer consumes trade order messages from standard input
 * and writes the updates to buy-values and sell-values to 
 * standard output.
 * 
 * @author yuan
 *
 */
public class Pricer {
	
	private Long targetSize;
	private BigDecimal lastTotalSaleValue;
	private BigDecimal lastTotalBuyValue; 
	private OrderBook askBook;
	private OrderBook bidBook;
	private Long timeStampMilliSec; //time stamp of the latest order consumed
	
	/**
	 * Instantiate a Pricer object with the specified targetSize.
	 * 
	 * @param targetSize - size (in shares) of the target trade
	 */
	public Pricer(Long targetSize){
		this.targetSize = targetSize;
		this.askBook = new OrderBook(Side.ASK, targetSize);
		this.bidBook = new OrderBook(Side.BID, targetSize);
		this.lastTotalBuyValue = new BigDecimal(-1); //-1 signifies that a buy order at target-size cannot be executed
		this.lastTotalSaleValue = new BigDecimal(-1); //-1 signifies that a sell order at target-size cannot be executed
	}
	
	/**
	 * consumes trade messages from standard-input and write updates to sale-value
	 * and buy-value to standard-output
	 */
	public void run(){
		
		Scanner scanner = new Scanner(System.in);
		while(scanner.hasNextLine()){
			String tradeMsg = scanner.nextLine();
			if(tradeMsg == null){
				break;
			}
			try {
				this.consumeOrderAndProduceOutput(tradeMsg);
			} catch (TradeOrderException e) {
				System.err.println("WARNING: Invalid trade message received. Ignored.");
			}
		}
		scanner.close();
	}
	
	/**
	 * Given a string representation of a new order or a 
	 * reduction order, consume the order message by updating
	 * the underlying representation of the order book. Also,
	 * if the sale value or buy value has changed since the last
	 * call to this method, print the update messages to standard
	 * output.
	 * <p>
	 * NOTE: if it is impossible to buy, the buy value is -1. 
	 * Similarly, if it is impossible to sell, the sell value is
	 * represented by -1.
	 * 
	 * @param orderStr - string representation of a new add order or a reduce order
	 * @throws TradeOrderException - if the trade-order does not follow
	 * the expected schema
	 */
	public void consumeOrderAndProduceOutput(String orderStr) throws TradeOrderException{
		this.consumeOrderString(orderStr);
		BigDecimal updatedBuyValue = this.updateBuyValue();
		BigDecimal updatedSaleValue = this.updateSaleValue();
		
		if(updatedBuyValue != null){
			System.out.println(Objects.toString(this.timeStampMilliSec) + " B " + Objects.toString(updatedBuyValue).replace("-1", "NA"));
		}
		if(updatedSaleValue != null){
			System.out.println(Objects.toString(this.timeStampMilliSec) + " S " + Objects.toString(updatedSaleValue).replace("-1", "NA"));
		}
	}
	
	/**
	 * Given a string representation of a new order or a 
	 * reduction order, consume the order message by updating
	 * the underlying bid-book or ask-book.
	 * 
	 * @param orderStr - string representation of a new add order or a reduce order
	 * @throws TradeOrderException - if the trade-order does not follow
	 * the expected schema
	 */
	private void consumeOrderString(String orderStr) throws TradeOrderException{
		try{
			String[] orderStrArray = orderStr.split("\\s");
			Long timeStampMilliSecs = Long.parseLong(orderStrArray[0]);
			String tradeID = orderStrArray[2];
			
			this.timeStampMilliSec = timeStampMilliSecs;
			if(orderStrArray[1].equals("A")){ //new order
				BigDecimal price = new BigDecimal(orderStrArray[4]);
				Long orderSize = Long.parseLong(orderStrArray[5]);
				if(orderStrArray[3].equals("B")){ // new buy/bid order
					this.bidBook.processNewOrder(tradeID, timeStampMilliSecs, Side.BID, price, orderSize);
				}else if(orderStrArray[3].equals("S")){// new sell/ask order
					this.askBook.processNewOrder(tradeID, timeStampMilliSecs, Side.ASK, price, orderSize);
				}else{
					throw new TradeOrderException("Unrecognized order string - bid/ask not properly encoded.");
				}
			}else if(orderStrArray[1].equals("R")){ //reduction order
				Long reductionSize = Long.parseLong(orderStrArray[3]);
				if(this.bidBook.contains(tradeID)){
					this.bidBook.processReduceOrder(tradeID, timeStampMilliSecs, reductionSize);
				}else if(this.askBook.contains(tradeID)){
					this.askBook.processReduceOrder(tradeID, timeStampMilliSecs, reductionSize);
				}else{
					throw new TradeOrderException("Reduce order does not match any previous add order.");
				}
			}else{
				throw new TradeOrderException("Unrecognized order string - add/reduce not properly encoded.");
			}
		}catch (NumberFormatException e){
			throw new TradeOrderException("Unrecognized order string - add/reduce not properly encoded.");
		}
	}
	
	/**
	 * Check whether the sale value has changed since the last time this method
	 * was called. If no change, return null. Otherwise, return the new value.
	 * 
	 * <p>
	 * NOTE: if it is impossible to sell, the sell value is -1. 
	 * 
	 * @return
	 */
	private BigDecimal updateSaleValue(){
		BigDecimal val = this.bidBook.computeValue();
		if(!Objects.equals(val, this.lastTotalSaleValue)){
			this.lastTotalSaleValue = val;
			return val;
		}else{
			return null;
		}
	}
	
	/**
	 * Check whether the buy value has changed since the last time this method
	 * was called. If no change, return null. Otherwise return the new value.
	 * 
	 * <p>
	 * NOTE: if it is impossible to buy, the buy value is -1. 
	 *
	 * 
	 * @return
	 */
	private BigDecimal updateBuyValue(){
		BigDecimal val = this.askBook.computeValue();
		if(!Objects.equals(val, this.lastTotalBuyValue)){
			this.lastTotalBuyValue = val;
			return val;
		}else{
			return null;
		}
	}
	
	public static void main(String[] args) throws TradeOrderException{
		if(args.length==1){
			Long targetSize = Long.parseLong(args[0]);
			Pricer pricer = new Pricer(targetSize);
			pricer.run();
		}else{
			System.err.println("Did not receive expected number of command line arguments [1] for target-size");
		}
	}
	

}
