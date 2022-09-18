(ns task-jockey.children)

(defn add-child [children group worker-id task-id child]
  (assoc-in children [group worker-id] {:task task-id :child child}))

(defn get-child [children task-id]
  (->> (for [[_ pool] children
             [_ {:keys [task child]}] pool
             :when (= task task-id)]
         child)
       first))

(defn has-active-tasks? [children]
  (some (fn [[_ pool]] (boolean (seq pool))) children))

(defn next-group-worker [children group]
  (let [pool (get children group)]
    (or (->> pool
             (keep-indexed (fn [i [worker-id _]]
                             (when (not= i worker-id)
                               i)))
             first)
        (count pool))))
