package com.dataanalytics.modelBeans;

public class HotelModelBean {

	String name;
	String city;
	String rank;
	String rating;
	String reviewCount;
	String url;

	public HotelModelBean(String name, String city, String rank, String rating, String reviewCount, String url) {
		super();
		this.name = name;
		this.city = city;
		this.rank = rank;
		this.rating = rating;
		this.reviewCount = reviewCount;
		this.url = url;
	}

	public String getName() {
		return name;
	}

	public String getCity() {
		return city;
	}

	public String getRank() {
		return rank;
	}

	public String getRating() {
		return rating;
	}

	public String getReviewCount() {
		return reviewCount;
	}

	public String getUrl() {
		return url;
	}
}
