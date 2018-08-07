package reliable_multicast.utils;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class StepProcessMap {
    private Map<Integer, Set<Integer>> steps;

    public StepProcessMap() {
        super();
        this.steps = new HashMap<>();
    }

    public StepProcessMap(Map<Integer, Set<String>> stepsMap) {
        this();
        this.fromMap(stepsMap);
    }

    public void addStep(Integer stepNumber) {
        this.steps.put(stepNumber, new HashSet<>());
    }

    public void removeStep(Integer stepNumber) {
        this.steps.remove(stepNumber);
    }

    public boolean addProcess(Integer stepNumber, Integer processId) {
        Set<Integer> processes = this.steps.get(stepNumber);
        if (processes == null)
            return false;
        return processes.add(processId);
    }

    public Set<Integer> getProcessesInStep(Integer step) {
        if (!this.steps.keySet().contains(step))
            return null;
        return new HashSet<Integer>(this.steps.get(step));
    }

    public void fromMap(Map<Integer, Set<String>> stepsMap) {
        if (stepsMap == null)
            return;

        Integer tmpStep;
        Pattern labelPattern = Pattern.compile("^p([0-9]+)$");

        Iterator<Integer> stepIterator =
                stepsMap.keySet().iterator();
        while (stepIterator.hasNext()) {
            tmpStep = stepIterator.next();
            this.addStep(tmpStep);

            String tmpProcess;
            Iterator<String> processIterator =
                    stepsMap.get(tmpStep).iterator();
            while (processIterator.hasNext()) {
                tmpProcess = processIterator.next();
                Matcher processMatch = labelPattern.matcher(tmpProcess);
                if (processMatch.matches()) {
                    this.addProcess(
                            tmpStep,
                            Integer.parseInt(processMatch.group(1)));
                }
            }
        }
    }
}
