#' Function to sort a set of points/coordinates in a clockwise or anti-clockwise
#' direction.
#'
#' \code{sort_points} can be useful for creating spatial lines or polygons from
#' points.
#'
#' @author Stuart K. Grange
#'
#' @param df Data frame containing y and x points.
#' @param y Name of y variable in \code{df}.
#' @param x Name of x variable in \code{df}.
#' @param clockwise Should the points be arranged in a clockwise direction? If
#' \code{FALSE}, the order will be an anti-clockwise direction.
#'
#' @export
sort_points <- function(df, y, x, clockwise = TRUE) {

  # Get centre (-oid) point of points
  x_centre <- mean(x)
  y_centre <- mean(y)

  # Calculate deltas
  df$x_delta <- x - x_centre
  df$y_delta <- y - y_centre

  # Resolve angle, in radians
  df$angle <- atan2(df$y_delta, df$x_delta)
  # d$angle_degrees <- d$angle * 180 / pi

  # Arrange by angle
  if (clockwise) {

    df <- df[order(df$angle, decreasing = TRUE), ]

  } else {

    df <- df[order(df$angle, decreasing = FALSE), ]

  }

  # Drop intermediate variables
  df[, c("x_delta", "y_delta", "angle")] <- NULL

  # Return
  df

}
