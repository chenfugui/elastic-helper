package com.esclient;

import java.util.Collection;

/**
 * 
 * @author chenfg
 * es items query by id vo
 *
 */
public class EsItemsVo {

	private String index;
	private String type;
	private Collection<String> idsCollect;
	
	public EsItemsVo()
	{
		
	}
	
	public EsItemsVo(String index,String type,Collection<String> idsCollect)
	{
		this.index=index;
		this.type=type;
		this.idsCollect=idsCollect;
	}

	public String getIndex() {
		return index;
	}

	public void setIndex(String index) {
		this.index = index;
	}

	public String getType() {
		return type;
	}

	public void setType(String type) {
		this.type = type;
	}

	public Collection<String> getIdsCollect() {
		return idsCollect;
	}

	public void setIdsCollect(Collection<String> idsCollect) {
		this.idsCollect = idsCollect;
	}

	@Override
	public String toString() {
		return "EsItemsVo [index=" + index + ", type=" + type + ", idsCollect="
				+ idsCollect + "]";
	}
	
	
}
