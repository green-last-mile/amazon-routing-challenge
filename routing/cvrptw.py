from ortools.constraint_solver import routing_enums_pb2
from ortools.constraint_solver import pywrapcp

class GoogleOR:
	def __init__(self, data, timelimit):
		# Just the data is needed to initialize
		self.data = data
		self.timelimit = timelimit
	def solve(self):
		data = self.data
		"""Solve the VRP with time windows."""
		manager = pywrapcp.RoutingIndexManager(len(data[’time_matrix’]),
		data[’num_vehicles’], data[’depot’])
		routing = pywrapcp.RoutingModel(manager)

		def time_callback(from_index, to_index):
			"""Returns the travel time between the two nodes."""
			# Convert from routing variable Index to time matrix NodeIndex.
			from_node = manager.IndexToNode(from_index)
			to_node = manager.IndexToNode(to_index)
			return data[’time_matrix’][from_node][to_node]

		transit_callback_index = routing.RegisterTransitCallback(time_callback)
		routing.SetArcCostEvaluatorOfAllVehicles(transit_callback_index)
		time = ’Time’
		routing.AddDimension(
		transit_callback_index,
		600, # allow waiting time
		1800, # maximum time per vehicle
		False, # Don’t force start cumul to zero.
		time)
		time_dimension = routing.GetDimensionOrDie(time)
		time_dimension.SetSpanCostCoefficientForAllVehicles(1000)

		# Add Capacity constraint.
		def demand_callback(from_index):
		"""Returns the demand of the node."""
		# Convert from routing variable Index to demands NodeIndex.
		from_node = manager.IndexToNode(from_index)
		return data[’demands’][from_node]
		demand_callback_index = routing.RegisterUnaryTransitCallback(
		demand_callback)
		routing.AddDimensionWithVehicleCapacity(
		demand_callback_index,
		0, # null capacity slack
		data[’vehicle_capacities’], # vehicle maximum capacities
		True, # start cumul to zero
		’Capacity’)
		# Add time window constraints for each location except depot.
		for location_idx, time_window in enumerate(data[’time_windows’]):
		if location_idx == 0:
		continue
		index = manager.NodeToIndex(location_idx)
		time_dimension.CumulVar(index).SetRange(time_window[0], time_window[1])
		# Add time window constraints for each vehicle start node.
		for vehicle_id in range(data[’num_vehicles’]):
		index = routing.Start(vehicle_id)
		time_dimension.CumulVar(index).SetRange(data[’time_windows’][0][0],
		data[’time_windows’][0][1])
		time_dimension.SetSpanUpperBoundForVehicle(600, vehicle_id) # This line limits
		vehicle driving time
		for i in range(data[’num_vehicles’]):
		routing.AddVariableMinimizedByFinalizer(
		time_dimension.CumulVar(routing.End(i)))
		search_parameters = pywrapcp.DefaultRoutingSearchParameters()
		search_parameters.first_solution_strategy = (
		routing_enums_pb2.FirstSolutionStrategy.AUTOMATIC)
		search_parameters.local_search_metaheuristic = (
		routing_enums_pb2.LocalSearchMetaheuristic.TABU_SEARCH)
		search_parameters.time_limit.seconds = self.timelimit
		solution = routing.SolveWithParameters(search_parameters)
		print("Solver status: ", routing.status())
		# Passing solution to other functions in this class
		self.solution = solution
		self.data = data
		self.routing = routing
		self.manager = manager
