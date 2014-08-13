package com.lunex.loadtest;

import com.fasterxml.jackson.annotation.JsonProperty;

public class CrudRequest {

    String username;
    String crud;
    Boolean isCommit;
    Boolean isUseContext;
    
    public CrudRequest() {
    }

    public CrudRequest(String username, String crud, Boolean isCommit, Boolean isUseContext) {
        this.username = username;
        this.crud = crud;
        this.isCommit = isCommit;
        this.isUseContext = isUseContext;
    }

    @JsonProperty
	public String getUsername() {
		return username;
	}

	public void setUsername(String username) {
		this.username = username;
	}

	@JsonProperty
	public String getCrud() {
		return crud;
	}

	public void setCrud(String crud) {
		this.crud = crud;
	}

	@JsonProperty
	public Boolean getIsCommit() {
		return isCommit;
	}

	public void setIsCommit(Boolean isCommit) {
		this.isCommit = isCommit;
	}

	@JsonProperty
	public Boolean getIsUseContext() {
		return isUseContext;
	}

	public void setIsUseContext(Boolean isUseContext) {
		this.isUseContext = isUseContext;
	}
}