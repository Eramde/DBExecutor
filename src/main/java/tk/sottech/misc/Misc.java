/*
 * Copyright (c) 2016, sot
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 * * Redistributions of source code must retain the above copyright notice, this
 *   list of conditions and the following disclaimer.
 * * Redistributions in binary form must reproduce the above copyright notice,
 *   this list of conditions and the following disclaimer in the documentation
 *   and/or other materials provided with the distribution.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 */
package tk.sot_tech.misc;

/**
 *
 * @author sot
 */
public class Misc {
	
	private static String OWN_PACKAGE = "tk.sot_tech";
	
	public static void setOwnPackage(String pkg){
		if(pkg != null) OWN_PACKAGE = pkg;
	}

	public static String ownStack(Throwable t) {
		StringBuilder sb = new StringBuilder("Exception ");
		sb.append(t.toString()).append(":\n");
		StackTraceElement[] stackTrace = t.getStackTrace();
		if (stackTrace != null) {
			for (StackTraceElement ste : stackTrace) {
				if (ste != null && nullToEmpty(ste.getClassName()).startsWith(OWN_PACKAGE)) {
					if (sb.charAt(sb.length() - 1) == '.') {
						sb.append("\n");
					}
					sb.append("\t@").append(ste.toString()).append('\n');
				}
				else {
					if (sb.charAt(sb.length() - 1) == '\n') {
						sb.append("\t");
					}
					sb.append('.');
				}
			}
		}
		return sb.toString();
	}

	private static String nullToEmpty(String str) {
		return str == null ? "" : str;
	}
}
