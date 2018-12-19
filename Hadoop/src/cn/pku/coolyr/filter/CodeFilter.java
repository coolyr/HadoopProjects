package cn.pku.coolyr.filter;

import java.io.IOException;


import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServlet;


public class CodeFilter extends HttpServlet implements Filter
{
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private String encoding;

	@Override
	public void doFilter(ServletRequest request, ServletResponse response, FilterChain filterChain) throws IOException, ServletException
	{

		// TODO 自动生成的方法存根
		System.out.println("this is Code filter !");
		//设置接收编码
		request.setCharacterEncoding(encoding);
		//请求继续转发
		response.setCharacterEncoding(encoding);
		filterChain.doFilter(request, response);
	}

	@Override
	public void init(FilterConfig filterConfig) throws ServletException
	{
		// TODO 自动生成的方法存根
		encoding = filterConfig.getInitParameter("encoding");
	}

}
