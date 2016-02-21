fromStream("DummyEvents")
  .when({
    "$init": function () {
      return { total: 0, sum: 0, avg: 0 }
    },
    "DummyEventA": function (state, ev) {
      state.total++;
      state.sum += ev.data.Value;
      state.avg = state.sum / state.total;
    }
  });