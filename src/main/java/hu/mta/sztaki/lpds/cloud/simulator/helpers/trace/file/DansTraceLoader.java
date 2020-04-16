package hu.mta.sztaki.lpds.cloud.simulator.helpers.trace.file;

import java.lang.reflect.InvocationTargetException;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

import hu.mta.sztaki.lpds.cloud.simulator.helpers.job.Job;

public class DansTraceLoader extends TraceFileReaderFoundation {

	public DansTraceLoader(String fileName, int from, int to, boolean furtherjobs, Class<? extends Job> jobType)
			throws SecurityException, NoSuchMethodException {
		super("Grid workload format", fileName, from, to, furtherjobs, jobType);
		// TODO Auto-generated constructor stub
	}

	protected boolean isTraceLine(final String param) {
		boolean isValid = false;
		String[] test = param.split(" ");

		if (test[0].equals(test[0].valueOf(0))) {
			if (test[1].equals(test[1].valueOf(1))) {
				if (test[3].equalsIgnoreCase("default")) {
					isValid = true;
				} else if (test[3].equalsIgnoreCase("url")) {
					isValid = true;
				} else if (test[3].equalsIgnoreCase("export")) {
					isValid = true;

				}
			}
		}

		if (isValid = true) {
			return true;
		} else
			return false;

	}

	protected void metaDataCollector(String data) {

	}

	protected Job createJobFromLine(String job)
			throws IllegalArgumentException, InstantiationException, IllegalAccessException, InvocationTargetException {

		String[] elements = job.trim().split("\\s+");

		if (elements[2].contains("error:unsupported-request-method")) {
			return null;
		}
		long jobState = Long.parseLong(elements[0]);
		int procs = ThreadLocalRandom.current().nextInt(4, 8); // Hard coded value for test purposes
		long runtime = 400;
		long waitTime = 0;
		String name = elements[2];

		String[] values = name.split("w");
		String wanted = values[0];
		wanted = wanted.substring(0, wanted.length() - 1);

		String submitTime = elements[1];
		submitTime = submitTime.substring(0, submitTime.indexOf("."));
		long convertSubmitToLong = Long.parseLong(submitTime);

		if (jobState != 1 && (procs < 1 || runtime < 0)) {
			return null;
		} else {
			return jobCreator.newInstance(
					// id
					elements[0],
					// submit time:

					convertSubmitToLong,
					// queueing time:
					Math.max(0, waitTime),
					// execution time:
					Math.max(0, runtime),
					// Number of processors
					Math.max(1, procs),
					// average execution time
					500,
					// no memory
					300,

					// User name:
					parseTextualField(wanted),
					// Group membership:
					parseTextualField(wanted),
					// executable name:
					parseTextualField(wanted),
					// No preceding job
					null, 0);
		}
	}

	private String parseTextualField(final String unparsed) {
		return unparsed.equals("-1") ? "N/A" : unparsed;
		// unparsed.matches("^-?[0-9](?:\\.[0-9])?$")?"N/A":unparsed;
	}

}