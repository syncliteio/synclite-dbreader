/*
 * Copyright (c) 2024 mahendra.chavan@synclite.io, all rights reserved.
 *
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied.  See the License for the specific language governing permissions and limitations
 * under the License.
 *
 */

package com.synclite.dbreader.web;

import java.io.IOException;
import java.security.SecureRandom;
import java.util.Base64;

import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.annotation.WebFilter;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.HttpSession;

@WebFilter("/*")
public class CSRFTokenFilter implements Filter {

	private static final String CSRF_TOKEN_ATTR = "csrfToken";
	private static final String CSRF_TOKEN_PARAM = "csrfToken";
	private static final SecureRandom RANDOM = new SecureRandom();

	@Override
	public void init(FilterConfig filterConfig) throws ServletException {
	}

	@Override
	public void doFilter(ServletRequest request, ServletResponse response, FilterChain chain)
			throws IOException, ServletException {
		HttpServletRequest httpRequest = (HttpServletRequest) request;
		HttpServletResponse httpResponse = (HttpServletResponse) response;

		HttpSession session = httpRequest.getSession(true);

		if ("POST".equalsIgnoreCase(httpRequest.getMethod())) {
			String sessionToken = (String) session.getAttribute(CSRF_TOKEN_ATTR);
			String requestToken = httpRequest.getParameter(CSRF_TOKEN_PARAM);
			if (sessionToken == null || !sessionToken.equals(requestToken)) {
				httpResponse.sendError(HttpServletResponse.SC_FORBIDDEN, "Invalid or missing CSRF token.");
				return;
			}
			// Rotate token after successful validation (single-use)
			session.setAttribute(CSRF_TOKEN_ATTR, generateToken());
		}

		// Generate token only if one doesn't already exist in the session
		if (session.getAttribute(CSRF_TOKEN_ATTR) == null) {
			session.setAttribute(CSRF_TOKEN_ATTR, generateToken());
		}

		// Set security headers
		httpResponse.setHeader("X-Content-Type-Options", "nosniff");
		httpResponse.setHeader("X-Frame-Options", "DENY");
		httpResponse.setHeader("X-XSS-Protection", "1; mode=block");
		httpResponse.setHeader("Cache-Control", "no-store, no-cache, must-revalidate");
		httpResponse.setHeader("Pragma", "no-cache");

		chain.doFilter(request, response);
	}

	@Override
	public void destroy() {
	}

	private static String generateToken() {
		byte[] bytes = new byte[32];
		RANDOM.nextBytes(bytes);
		return Base64.getUrlEncoder().withoutPadding().encodeToString(bytes);
	}
}
