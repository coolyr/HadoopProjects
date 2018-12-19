<%@ page language="java" import="java.util.*" pageEncoding="utf-8"%>
<%@ taglib prefix="c" uri="http://java.sun.com/jsp/jstl/core"%>
<!DOCTYPE html PUBLIC "-//W3C//DTD XHTML 1.0 Transitional//EN" "http://www.w3.org/TR/xhtml1/DTD/xhtml1-transitional.dtd">
<html>
  <head> 
    <title>主页面</title>
    <meta http-equiv="pragma" content="no-cache"/>
	<meta http-equiv="cache-control" content="no-cache"/>
	<meta http-equiv="expires" content="0"/> 
    <link rel="stylesheet" href="/Hadoop/css/common.css" type="text/css"/>
  <link rel="stylesheet" href="/Hadoop/css/main.css" type="text/css"/>
  
  
  <script type="text/javascript" src="js/myJs.js"></script>
  <script type="text/javascript" src="js/jquery-1.4.2.js"></script>
  
  </head>
  <body>
  	<!-- 引入多页面 -->
  	<jsp:include page="head.jsp" />
    <div class="main">
	<div class="infor">
	<div class="infor_fileup">
	<div class="infor_fileup_loc">
	  <!-- 上传type="file"类型的文件必须修改表单的类型：enctype="multipart/form-data" -->
    <form action="/Hadoop/file.do?flag=fileUp" enctype="multipart/form-data" method="post" onsubmit="return checkFile()">
	<a href="javascript:;" class="file">点击这里上传文件<input type="file" name="myfile" id="myfile"/></a>
	<input type="image"  title="上传" style="width: 27px; margin-left: 5px;" src="/Hadoop/images/upfile.jpg" value="上传"/>
	<span id="fileHint" style=" color: red; display: none; margin-left: 12px;">请选择一个文件！</span>
	</form>
	</div>
	</div>
	<div class="infor_fileinfor">
	<table>
	<tr><td class="infor_fileinfor_table_td1">文件名：</td><td class="infor_fileinfor_table_td2"><span class="font2"><c:out value="${session_old_name}" default="未上传" escapeXml="false"></c:out></span></td></tr>
	<tr><td class="infor_fileinfor_table_td1">大小：</td><td class="infor_fileinfor_table_td2"><span class="font2"><c:out value="${session_file_size}" default="未上传" escapeXml="false"></c:out></span></td></tr>
	<tr><td class="infor_fileinfor_table_td1">行数：</td><td class="infor_fileinfor_table_td2"><span class="font2"><c:out value="${session_line_num}" default="未上传" escapeXml="false"></c:out></span></td></tr>
	<tr><td class="infor_fileinfor_table_td1">列数：</td><td class="infor_fileinfor_table_td2"><span class="font2"><c:out value="${session_column_num}" default="未上传" escapeXml="false"></c:out></span></td></tr>
	</table>
	
	</div>
	</div>
	<div class="infor_right">
	<div class="infor_right_config">
	<form action="#" method="post">
	 	<div  class="sort_column">
		排序权重一
		<select name="sort_column_1">
			<option value="1">column 1</option>
			<option value="2">column 2</option>
			<option value="3">column 3</option>
			<option value="4">column 4</option>
			<option value="5">column 5</option>
		</select>
		<br/>
		</div>
		 <div class="sort_column">
		排序权重二
		<select name="sort_column_2">
			<option value="1">column 1</option>
			<option value="2" selected="selected">column 2</option>
			<option value="3">column 3</option>
			<option value="4">column 4</option>
			<option value="5">column 5</option>
		</select>
		<br/>
		</div>
		 <div class="sort_column">
		排序权重三
		<select name="sort_column_3">
			<option value="1">column 1</option>
			<option value="2">column 2</option>
			<option value="3" selected="selected">column 3</option>
			<option value="4">column 4</option>
			<option value="5">column 5</option>
		</select>
		<br/>
		</div>
		<div class="sort_column" style="margin-left: 35px;">
		<input type="submit" value="排序" />
		</div>
	</form>
	</div>
	<div class="infor_right_output">
		<c:forEach items="${session_contentList}" var="line">
    	 ${line} <br />
		</c:forEach>
	</div>
	</div>
    </div>
    <jsp:include page="foot.jsp" />
  </body>
</html>
